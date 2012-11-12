#!/usr/bin/env python

import re
import os
import os.path
import stat
import sys
import tempfile
import argparse


# Functions to convert a list of arguments to a quoted shell command, provided by Dave Abrahams
# http://stackoverflow.com/questions/967443/python-module-to-shellquote-unshellquote
_quote_pos = re.compile('(?=[^-0-9a-zA-Z_./\n])')
def shQuote(arg):
    r"""Quote the argument for the shell.

    >>> quote('\t')
    '\\\t'
    >>> quote('foo bar')
    'foo\\ bar'
    """
    # This is the logic emacs uses
    if arg:
        return _quote_pos.sub('\\\\', arg).replace('\n',"'\n'")
    else:
        return "''"
def shCommandFromArgs(args):
    """Convert a list of shell arguments to a shell command-line"""
    return ' '.join([shQuote(a) for a in args])


class PbsArgumentParser(argparse.ArgumentParser):
    """An argument parser to get relevant parameters for PBS."""
    def __init__(self, parent=None, *args, **kwargs):
        super(PbsArgumentParser, self).__init__(*args, **kwargs)
        self.parent = parent
        self.add_argument("--queue", help="PBS queue name")
        self.add_argument("--job", help="Job name")
        self.add_argument("--nodes", type=int, default=1, help="Number of nodes")
        self.add_argument("--procs", type=int, default=1, help="Number of processors per node")
        self.add_argument("--time", type=float, help="Expected execution time per processor (sec)")
        self.add_argument("--pbs-output", dest="pbsOutput", help="Output directory")
        self.add_argument("--dry-run", dest="dryrun", default=False, action="store_true",
                          help="Dry run?")
        self.add_argument("--do-exec", dest="doExec", default=False, action="store_true",
                          help="Exec script instead of qsub?")
        self.add_argument("--mpiexec", default="", help="mpiexec options")

    def parse_args(self, *args, **kwargs):
        args, leftover = super(PbsArgumentParser, self).parse_known_args(*args, **kwargs)
        if len(leftover) > 0:
            # Ensure the parent can parse the leftovers
            if self.parent is None:
                self.error("Unrecognised arguments: %s" % leftover)
            args.leftover = leftover
            args.parent = self.parent.parse_args(args=leftover)
        args.pbs = Pbs(outputDir=args.pbsOutput, numNodes=args.nodes, numProcsPerNode=args.procs,
                       queue=args.queue, jobName=args.job, time=args.time, dryrun=args.dryrun,
                       doExec=args.doExec, mpiexec=args.mpiexec)
        return args

    def formatParentUsage(self):
        if self.parent is None:
            return
        usage = """
The following options can also be added, which will be directed to the
underlying process' ArgumentParser (%s):
""" % type(self.parent).__name__)
        usage += self.parent.format_usage()
        return usage

    def format_usage(self):
        super(PbsArgumentParser, self).format_usage()
        self.formatParentUsage()

    def format_help(self):
        super(PbsArgumentParser, self).format_help()
        self.formatParentUsage()


class Pbs(object):
    def __init__(self, outputDir=None, numNodes=1, numProcsPerNode=1, queue=None, jobName=None, time=None,
                 dryrun=False, doExec=False, mpiexec=""):
        self.outputDir = outputDir
        self.numNodes = numNodes
        self.numProcsPerNode = numProcsPerNode
        self.queue = queue
        self.jobName = jobName
        self.time = time
        self.dryrun = dryrun
        self.doExec = doExec
        self.mpiexec = mpiexec

    def create(self, command, repeats=1, time=None, numNodes=None, numProcsPerNode=None, jobName=None,
               threads=None):
        if time is None:
            time = self.time
        if numNodes is None:
            numNodes = self.numNodes
        if numProcsPerNode is None:
            numProcsPerNode = self.numProcsPerNode
        if jobName is None:
            jobName = self.jobName
        if threads is None:
            threads = numNodes * numProcsPerNode
        threads = min(threads, numNodes * numProcsPerNode)

        fd, script = tempfile.mkstemp()
        f = os.fdopen(fd, "w")

        if numNodes is None or numProcsPerNode is None:
            raise RuntimeError("numNodes (%s) or numProcsPerNode (%s) is not specified" %
                               (numNodes, numProcsPerNode))

        assert numNodes is not None and numProcsPerNode is not None
        if jobName is None:
            # Name of executable without path
            jobName = command[:command.find(" ")]
            jobName = jobName[jobName.rfind("/"):]

        print >>f, "#!/bin/bash"
        print >>f, "#   Post this job with `qsub -V $0'"
        print >>f, "#PBS -l nodes=%d:ppn=%d" % (numNodes, numProcsPerNode)
        if time is not None:
            wallTime = repeats * time / threads
            print >>f, "#PBS -l walltime=%d" % wallTime
        if self.outputDir is not None:
            print >>f, "#PBS -o %s" % self.outputDir
        print >>f, "#PBS -N %s" % jobName
        if self.queue is not None:
            print >>f, "#PBS -q %s" % self.queue
        print >>f, "#PBS -j oe"
        print >>f, "#PBS -W umask=02"
        print >>f, exportEnv()
        print >>f, "echo \"mpiexec is at: $(which mpiexec)\""
        print >>f, "ulimit -a"
        print >>f, "umask 02"
        print >>f, "echo 'umask: ' $(umask)"
        print >>f, "eups list -s"
        print >>f, "export"
        print >>f, "cd %s" % os.getcwd()
        print >>f, "mpiexec --verbose %s %s" % (self.mpiexec, command)
        f.close()
        os.chmod(script, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        return script

    def run(self, command, *args, **kwargs):
        script = self.create(command, *args, **kwargs)
        command = "qsub -V %s" % script
        if self.dryrun:
            print "Would run: %s" % command
        elif self.doExec:
            os.execl(script, script)
        else:
            os.system(command)
        return script


def exportEnv():
    """Generate bash script to regenerate the current environment"""
    output = ""
    for key, val in os.environ.items():
        if key in ("DISPLAY",):
            continue
        if val.startswith("() {"):
            # This is a function.
            # "Two parentheses, a single space, and a brace"
            # is exactly the same criterion as bash uses.
            output += "function {key} {val}\nexport -f {key}\n".format(key=key, val=val)
        else:
            # This is a variable.
            output += "export {key}='{val}'\n".format(key=key, val=val.replace("'", "'\"'\"'"))
    return output
