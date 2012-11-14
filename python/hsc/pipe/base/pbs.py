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

class PbsArgumentInfo(object):
    def __init__(self, action, args, kwargs, group=None):
        self.action = action
        self.args = args
        self.kwargs = kwargs
        self.group = group

class PbsArgumentGroup(object):
    def __init__(self, parser, group, title, args, kwargs):
        self.parser = parser
        self.group = group
        self.title = title
        self.args = args
        self.kwargs = kwargs

    def add_argument(self, *args, **kwargs):
        action = self.group.add_argument(*args, **kwargs)
        self.parser.recordArgument(action, args, kwargs, group=self.title)

class PbsArgumentParser(argparse.ArgumentParser):
    """An argument parser to get relevant parameters for PBS."""
    def __init__(self, parent=None, *args, **kwargs):
        self.parent = parent
        self.initArgs = args
        self.initKwargs = kwargs
        self.additionalGroups = {}
        self.additionalArgs = []
        self.defaultGroup = "Additional options"
        self.pbsGroup = "PBS options"
        super(PbsArgumentParser, self).__init__(*args, **kwargs)
        self.addPbsArguments()

    def addPbsArguments(self):
        group = super(PbsArgumentParser, self).add_argument_group(self.pbsGroup)
        group.add_argument("--queue", help="PBS queue name")
        group.add_argument("--job", help="Job name")
        group.add_argument("--nodes", type=int, default=1, help="Number of nodes")
        group.add_argument("--procs", type=int, default=1, help="Number of processors per node")
        group.add_argument("--time", type=float, help="Expected execution time per processor (sec)")
        group.add_argument("--pbs-output", dest="pbsOutput", help="Output directory")
        group.add_argument("--dry-run", dest="dryrun", default=False, action="store_true",
                           help="Dry run?")
        group.add_argument("--do-exec", dest="doExec", default=False, action="store_true",
                           help="Exec script instead of qsub?")
        group.add_argument("--mpiexec", default="", help="mpiexec options")

    def recordArgument(self, action, args, kwargs, group=None):
        self.additionalArgs.append(PbsArgumentInfo(action, args, kwargs, group=group))

    def add_argument(self, *args, **kwargs):
        if not self.defaultGroup in self.additionalGroups:
            self.add_argument_group(self.defaultGroup)
        action = self.additionalGroups[self.defaultGroup].add_argument(*args, **kwargs)
        self.recordArgument(action, args, kwargs)
        return action

    def add_argument_group(self, title, *args, **kwargs):
        if not title in self.additionalGroups:
            group = super(PbsArgumentParser, self).add_argument_group(title, *args, **kwargs)
            self.additionalGroups[title] = PbsArgumentGroup(self, group, title, args, kwargs)
        return self.additionalGroups[title]

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

    def mergeWithParent(self):
        if self.parent is None:
            return self
        kwargs = self.initKwargs.copy()
        parents = kwargs.pop("parents", [])
        parents.append(self.parent)
        merged = type(self)(*self.initArgs, parents=parents, conflict_handler="resolve", **self.initKwargs)
        groups = {}
        for info in self.additionalGroups.values():
            groups[info.title] = merged.add_argument_group(info.title, *info.args, **info.kwargs)
        for info in self.additionalArgs:
            adder = groups[info.group].add_argument if info.group is not None else merged.add_argument
            adder(*info.args, **info.kwargs)
        return merged

    def format_help(self):
        if self.parent is None:
            return super(PbsArgumentParser, self).format_help()
        return self.mergeWithParent().format_help()

    def format_usage(self):
        if self.parent is None:
            return super(PbsArgumentParser, self).format_usage()
        return self.mergeWithParent().format_usage()

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
