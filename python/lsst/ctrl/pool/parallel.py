#!/usr/bin/env python

import re
import os
import os.path
import stat
import sys
import pipes
import tempfile
import argparse
import traceback
import contextlib
from lsst.pipe.base import CmdLineTask, TaskRunner
from .pool import startPool, Pool, NODE, abortOnError, setBatchType
from . import log as dummyLog  # noqa

__all__ = ["Batch", "PbsBatch", "SlurmBatch", "SmpBatch", "BATCH_TYPES", "BatchArgumentParser",
           "BatchCmdLineTask", "BatchPoolTask", ]

UMASK = 0o002  # umask to set

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
        return _quote_pos.sub('\\\\', arg).replace('\n', "'\n'")
    else:
        return "''"


def shCommandFromArgs(args):
    """Convert a list of shell arguments to a shell command-line"""
    return ' '.join([shQuote(a) for a in args])


def processStats():
    """Collect Linux-specific process statistics

    Parses the /proc/self/status file (N.B. Linux-specific!) into a dict
    which is returned.
    """
    result = {}
    with open("/proc/self/status") as f:
        for line in f:
            key, _, value = line.partition(":")
            result[key] = value.strip()
    return result


def printProcessStats():
    """Print the process statistics to the log"""
    from lsst.log import Log
    log = Log.getDefaultLogger()
    log.info("Process stats for %s: %s" % (NODE, processStats()))


class Batch:
    """Base class for batch submission"""

    def __init__(self, outputDir=None, numNodes=0, numProcsPerNode=0, numCores=0, queue=None, jobName=None,
                 walltime=0.0, dryrun=False, doExec=False, mpiexec="", submit=None, options=None,
                 verbose=False):
        """!Constructor

        @param outputDir: output directory, or None
        @param numNodes: number of nodes
        @param numProcsPerNode: number of processors per node
        @param numCores: number of cores (Slurm, SMP only)
        @param queue: name of queue, or None
        @param jobName: name of job, or None
        @param walltime: maximum wall clock time for job
        @param dryrun: Dry run (only print actions that would be taken)?
        @param doExec: exec the script instead of submitting to batch system?
        @param mpiexec: options for mpiexec
        @param submit: command-line options for batch submission (e.g., for qsub, sbatch)
        @param options: options to append to script header (e.g., #PBS or #SBATCH)
        @param verbose: produce verbose output?
        """
        if (numNodes <= 0 or numProcsPerNode <= 0) and numCores <= 0:
            raise RuntimeError("Must specify numNodes+numProcs or numCores")

        self.outputDir = outputDir
        self.numNodes = numNodes
        self.numProcsPerNode = numProcsPerNode
        self.numCores = numCores
        self.queue = queue
        self.jobName = jobName
        self.walltime = walltime
        self.dryrun = dryrun
        self.doExec = doExec
        self.mpiexec = mpiexec
        self.submit = submit
        self.options = options
        self.verbose = verbose

    def shebang(self):
        return "#!/bin/bash"

    def preamble(self, command, walltime=None):
        """Return preamble string for script to be submitted

        Most batch systems allow you to embed submission options as comments here.
        """
        raise NotImplementedError("Not implemented for base class")

    def execution(self, command):
        """Return execution string for script to be submitted"""
        script = [exportEnv(),
                  "umask %03o" % UMASK,
                  "cd %s" % pipes.quote(os.getcwd()),
                  ]
        if self.verbose:
            script += ["echo \"mpiexec is at: $(which mpiexec)\"",
                       "ulimit -a",
                       "echo 'umask: ' $(umask)",
                       "eups list -s",
                       "export",
                       "date",
                       ]
        script += ["mpiexec %s %s" % (self.mpiexec, command)]
        if self.verbose:
            script += ["date",
                       "echo Done.",
                       ]
        return "\n".join(script)

    def createScript(self, command, walltime=None):
        """!Create script to be submitted

        @param command: command to run
        @param walltime: maximum wall clock time, overrides value to constructor
        @return name of script on filesystem
        """
        fd, scriptName = tempfile.mkstemp()
        with os.fdopen(fd, "w") as f:
            f.write(self.shebang())
            f.write('\n')
            f.write(self.preamble(walltime))
            f.write('\n')
            f.write(self.execution(command))
            f.write('\n')

        os.chmod(scriptName, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        return scriptName

    def submitCommand(self, scriptName):
        """!Return command to submit script

        @param scriptName: name of script on filesystem
        """
        raise NotImplementedError("No implementation for base class")

    def run(self, command, walltime=None):
        """!Run the batch system

        Creates and submits the script to execute the provided command

        @param command: command to run
        @param walltime: maximum wall clock time, overrides value to constructor
        @return name of script on filesystem
        """
        scriptName = self.createScript(command, walltime=walltime)
        command = self.submitCommand(scriptName)
        if self.dryrun:
            print("Would run: %s" % command)
        elif self.doExec:
            os.execl(scriptName, scriptName)
        else:
            os.system(command)
        return scriptName


class PbsBatch(Batch):
    """Batch submission with PBS"""

    def preamble(self, walltime=None):
        if walltime is None:
            walltime = self.walltime
        if walltime <= 0:
            raise RuntimeError("Non-positive walltime: %s (did you forget '--time'?)" % (walltime,))
        if self.numNodes <= 0 or self.numProcsPerNode <= 0:
            raise RuntimeError(
                "Number of nodes (--nodes=%d) or number of processors per node (--procs=%d) not set" %
                (self.numNodes, self.numProcsPerNode))
        if self.numCores > 0:
            raise RuntimeError("PBS does not support setting the number of cores")
        return "\n".join([
            "#PBS %s" % self.options if self.options is not None else "",
            "#PBS -l nodes=%d:ppn=%d" % (self.numNodes, self.numProcsPerNode),
            "#PBS -l walltime=%d" % walltime if walltime is not None else "",
            "#PBS -o %s" % self.outputDir if self.outputDir is not None else "",
            "#PBS -N %s" % self.jobName if self.jobName is not None else "",
            "#PBS -q %s" % self.queue if self.queue is not None else "",
            "#PBS -j oe",
            "#PBS -W umask=%03o" % UMASK,
        ])

    def submitCommand(self, scriptName):
        return "qsub %s -V %s" % (self.submit if self.submit is not None else "", scriptName)


class SlurmBatch(Batch):
    """Batch submission with Slurm"""

    @staticmethod
    def formatWalltime(walltime):
        """Format walltime (in seconds) as days-hours:minutes"""
        secInDay = 3600*24
        secInHour = 3600
        secInMinute = 60
        days = walltime//secInDay
        walltime -= days*secInDay
        hours = walltime//secInHour
        walltime -= hours*secInHour
        minutes = walltime//secInMinute
        walltime -= minutes*secInMinute
        if walltime > 0:
            minutes += 1
        return "%d-%d:%d" % (days, hours, minutes)

    def preamble(self, walltime=None):
        if walltime is None:
            walltime = self.walltime
        if walltime <= 0:
            raise RuntimeError("Non-positive walltime: %s (did you forget '--time'?)" % (walltime,))
        if (self.numNodes <= 0 or self.numProcsPerNode <= 0) and self.numCores <= 0:
            raise RuntimeError(
                "Number of nodes (--nodes=%d) and number of processors per node (--procs=%d) not set OR "
                "number of cores (--cores=%d) not set" % (self.numNodes, self.numProcsPerNode, self.numCores))
        if self.numCores > 0 and (self.numNodes > 0 or self.numProcsPerNode > 0):
            raise RuntimeError("Must set either --nodes,--procs or --cores: not both")

        outputDir = self.outputDir if self.outputDir is not None else os.getcwd()
        filename = os.path.join(outputDir, (self.jobName if self.jobName is not None else "slurm") + ".o%j")
        return "\n".join([("#SBATCH --nodes=%d" % self.numNodes) if self.numNodes > 0 else "",
                          ("#SBATCH --ntasks-per-node=%d" % self.numProcsPerNode) if
                          self.numProcsPerNode > 0 else "",
                          ("#SBATCH --ntasks=%d" % self.numCores) if self.numCores > 0 else "",
                          "#SBATCH --time=%s" % self.formatWalltime(walltime),
                          "#SBATCH --job-name=%s" % self.jobName if self.jobName is not None else "",
                          "#SBATCH -p %s" % self.queue if self.queue is not None else "",
                          "#SBATCH --output=%s" % filename,
                          "#SBATCH --error=%s" % filename,
                          "#SBATCH %s" % self.options if self.options is not None else "",
                          ])

    def submitCommand(self, scriptName):
        return "sbatch %s %s" % (self.submit if self.submit is not None else "", scriptName)


class SmpBatch(Batch):
    """Not-really-Batch submission with multiple cores on the current node

    The job is run immediately.
    """

    def __init__(self, *args, **kwargs):
        super(SmpBatch, self).__init__(*args, **kwargs)
        if self.numNodes in (0, 1) and self.numProcsPerNode > 0 and self.numCores == 0:
            # --nodes=1 --procs=NN being used as a synonym for --cores=NN
            self.numNodes = 0
            self.numCores = self.numProcsPerNode
            self.numProcsPerNode = 0
        if self.numNodes > 0 or self.numProcsPerNode > 0:
            raise RuntimeError("SMP does not support the --nodes and --procs command-line options; "
                               "use --cores to specify the number of cores to use")
        if self.numCores > 1:
            self.mpiexec = "%s -n %d" % (self.mpiexec if self.mpiexec is not None else "", self.numCores)
        else:
            self.mpiexec = ""

    def preamble(self, walltime=None):
        return ""

    def submitCommand(self, scriptName):
        return "exec %s" % scriptName


BATCH_TYPES = {'none': None,
               'None': None,
               'pbs': PbsBatch,
               'slurm': SlurmBatch,
               'smp': SmpBatch,
               }  # Mapping batch type --> Batch class


class BatchArgumentParser(argparse.ArgumentParser):
    """An argument parser to get relevant parameters for batch submission

    We want to be able to display the help for a 'parent' ArgumentParser
    along with the batch-specific options we introduce in this class, but
    we don't want to swallow the parent (i.e., ArgumentParser(parents=[parent]))
    because we want to save the list of arguments that this particular
    BatchArgumentParser doesn't parse, so they can be passed on to a different
    program (though we also want to parse them to check that they can be parsed).
    """

    def __init__(self, parent=None, *args, **kwargs):
        super(BatchArgumentParser, self).__init__(*args, **kwargs)
        self._parent = parent
        group = self.add_argument_group("Batch submission options")
        group.add_argument("--queue", help="Queue name")
        group.add_argument("--job", help="Job name")
        group.add_argument("--nodes", type=int, default=0, help="Number of nodes")
        group.add_argument("--procs", type=int, default=0, help="Number of processors per node")
        group.add_argument("--cores", type=int, default=0, help="Number of cores (Slurm/SMP only)")
        group.add_argument("--time", type=float, default=0,
                           help="Expected execution time per element (sec)")
        group.add_argument("--batch-type", dest="batchType", choices=list(BATCH_TYPES.keys()), default="smp",
                           help="Batch system to use")
        group.add_argument("--batch-verbose", dest="batchVerbose", action="store_true", default=False,
                           help=("Enable verbose output in batch script "
                                 "(including system environment information at batch start)?"))
        group.add_argument("--batch-output", dest="batchOutput", help="Output directory")
        group.add_argument("--batch-submit", dest="batchSubmit", help="Batch submission command-line flags")
        group.add_argument("--batch-options", dest="batchOptions", help="Header options for batch script")
        group.add_argument("--batch-profile", dest="batchProfile", action="store_true", default=False,
                           help="Enable profiling on batch job?")
        group.add_argument("--batch-stats", dest="batchStats", action="store_true", default=False,
                           help="Print process stats on completion (Linux only)?")
        group.add_argument("--dry-run", dest="dryrun", default=False, action="store_true",
                           help="Dry run?")
        group.add_argument("--do-exec", dest="doExec", default=False, action="store_true",
                           help="Exec script instead of submit to batch system?")
        group.add_argument("--mpiexec", default="", help="mpiexec options")

    def parse_args(self, config=None, args=None, namespace=None, **kwargs):
        args, leftover = super(BatchArgumentParser, self).parse_known_args(args=args, namespace=namespace)
        args.parent = None
        args.leftover = None
        if len(leftover) > 0:
            # Save any leftovers for the parent
            if self._parent is None:
                self.error("Unrecognised arguments: %s" % leftover)
            args.parent = self._parent.parse_args(config, args=leftover, **kwargs)
            args.leftover = leftover
        args.batch = self.makeBatch(args)
        return args

    def makeBatch(self, args):
        """Create a Batch object from the command-line arguments"""
        # argMapping is a dict that maps Batch init kwarg names to parsed arguments attribute *names*
        argMapping = {'outputDir': 'batchOutput',
                      'numNodes': 'nodes',
                      'numProcsPerNode': 'procs',
                      'numCores': 'cores',
                      'walltime': 'time',
                      'queue': 'queue',
                      'jobName': 'job',
                      'dryrun': 'dryrun',
                      'doExec': 'doExec',
                      'mpiexec': 'mpiexec',
                      'submit': 'batchSubmit',
                      'options': 'batchOptions',
                      'verbose': 'batchVerbose',
                      }

        if BATCH_TYPES[args.batchType] is None:
            return None

        # kwargs is a dict that maps Batch init kwarg names to parsed arguments attribute *values*
        kwargs = {k: getattr(args, v) for k, v in argMapping.items()}
        return BATCH_TYPES[args.batchType](**kwargs)

    def format_help(self):
        text = """This is a script for queue submission of a wrapped script.

Use this program name and ignore that for the wrapped script (it will be
passed on to the batch system).  Arguments for *both* this wrapper script or the
wrapped script are valid (if it is required for the wrapped script, it
is required for the wrapper as well).

*** Batch system submission wrapper:

"""
        text += super(BatchArgumentParser, self).format_help()
        if self._parent is not None:
            text += """

*** Wrapped script:

"""
            text += self._parent.format_help()
        return text

    def format_usage(self):
        if self._parent is not None:
            prog = self._parent.prog
            self._parent.prog = self.prog
            usage = self._parent.format_usage()
            self._parent.prog = prog
            return usage
        return super(BatchArgumentParser, self).format_usage()


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

            # From 2014-09-25, the function name is prefixed by 'BASH_FUNC_'
            # and suffixed by '()', which we have to remove.
            if key.startswith("BASH_FUNC_") and key.endswith("()"):
                key = key[10:-2]

            output += "{key} {val}\nexport -f {key}\n".format(key=key, val=val)
        else:
            # This is a variable.
            output += "export {key}='{val}'\n".format(key=key, val=val.replace("'", "'\"'\"'"))
    return output


class BatchCmdLineTask(CmdLineTask):

    @classmethod
    def parseAndSubmit(cls, args=None, **kwargs):
        taskParser = cls._makeArgumentParser(doBatch=True, add_help=False)
        batchParser = BatchArgumentParser(parent=taskParser)
        batchArgs = batchParser.parse_args(config=cls.ConfigClass(), args=args, override=cls.applyOverrides,
                                           **kwargs)

        if not cls.RunnerClass(cls, batchArgs.parent).precall(batchArgs.parent):  # Write config, schema
            taskParser.error("Error in task preparation")

        setBatchType(batchArgs.batch)

        if batchArgs.batch is None:     # don't use a batch system
            sys.argv = [sys.argv[0]] + batchArgs.leftover  # Remove all batch arguments

            return cls.parseAndRun()
        else:
            numCores = batchArgs.cores if batchArgs.cores > 0 else batchArgs.nodes*batchArgs.procs
            walltime = cls.batchWallTime(batchArgs.time, batchArgs.parent, numCores)

            command = cls.batchCommand(batchArgs)
            batchArgs.batch.run(command, walltime=walltime)

    @classmethod
    def batchWallTime(cls, time, parsedCmd, numCores):
        """!Return walltime request for batch job

        Subclasses should override if the walltime should be calculated
        differently (e.g., addition of some serial time).

        @param cls: Class
        @param time: Requested time per iteration
        @param parsedCmd: Results of argument parsing
        @param numCores: Number of cores
        """
        numTargets = len(cls.RunnerClass.getTargetList(parsedCmd))
        return time*numTargets/float(numCores)

    @classmethod
    def batchCommand(cls, args):
        """!Return command to run CmdLineTask

        @param cls: Class
        @param args: Parsed batch job arguments (from BatchArgumentParser)
        """
        job = args.job if args.job is not None else "job"
        module = cls.__module__
        script = ("import os; os.umask(%#05o); " +
                  "import lsst.base; lsst.base.disableImplicitThreading(); " +
                  "import lsst.ctrl.pool.log; lsst.ctrl.pool.log.jobLog(\"%s\"); ") % (UMASK, job)

        if args.batchStats:
            script += ("import lsst.ctrl.pool.parallel; import atexit; " +
                       "atexit.register(lsst.ctrl.pool.parallel.printProcessStats); ")

        script += "import %s; %s.%s.parseAndRun();" % (module, module, cls.__name__)

        profilePre = "import cProfile; import os; cProfile.run(\"\"\""
        profilePost = "\"\"\", filename=\"profile-" + job + "-%s-%d.dat\" % (os.uname()[1], os.getpid()))"

        return ("python -c '" + (profilePre if args.batchProfile else "") + script +
                (profilePost if args.batchProfile else "") + "' " + shCommandFromArgs(args.leftover) +
                " --noExit")

    @contextlib.contextmanager
    def logOperation(self, operation, catch=False, trace=True):
        """!Provide a context manager for logging an operation

        @param operation: description of operation (string)
        @param catch: Catch all exceptions?
        @param trace: Log a traceback of caught exception?

        Note that if 'catch' is True, all exceptions are swallowed, but there may
        be other side-effects such as undefined variables.
        """
        self.log.info("%s: Start %s" % (NODE, operation))
        try:
            yield
        except Exception:
            if catch:
                cls, e, _ = sys.exc_info()
                self.log.warn("%s: Caught %s while %s: %s" % (NODE, cls.__name__, operation, e))
                if trace:
                    self.log.info("%s: Traceback:\n%s" % (NODE, traceback.format_exc()))
                return
            raise
        finally:
            self.log.info("%s: Finished %s" % (NODE, operation))


class BatchPoolTask(BatchCmdLineTask):
    """Starts a BatchCmdLineTask with an MPI process pool

    Use this subclass of BatchCmdLineTask if you want to use the Pool directly.
    """
    @classmethod
    @abortOnError
    def parseAndRun(cls, *args, **kwargs):
        """Run with a MPI process pool"""
        pool = startPool()
        super(BatchPoolTask, cls).parseAndRun(*args, **kwargs)
        pool.exit()


class BatchTaskRunner(TaskRunner):
    """Run a Task individually on a list of inputs using the MPI process pool"""

    def __init__(self, *args, **kwargs):
        """Constructor

        Warn if the user specified multiprocessing.
        """
        TaskRunner.__init__(self, *args, **kwargs)
        if self.numProcesses > 1:
            self.log.warn("Multiprocessing arguments (-j %d) ignored since using batch processing" %
                          self.numProcesses)
            self.numProcesses = 1

    def run(self, parsedCmd):
        """Run the task on all targets

        Sole input is the result of parsing the command-line with the ArgumentParser.

        Output is None if 'precall' failed; otherwise it is a list of calling ourself
        on each element of the target list from the 'getTargetList' method.
        """
        resultList = None

        self.prepareForMultiProcessing()
        pool = Pool()

        if self.precall(parsedCmd):
            targetList = self.getTargetList(parsedCmd)
            if len(targetList) > 0:
                parsedCmd.log.info("Processing %d targets with a pool of %d processes..." %
                                   (len(targetList), pool.size))
                # Run the task using self.__call__
                resultList = pool.map(self, targetList)
            else:
                parsedCmd.log.warn("Not running the task because there is no data to process; "
                                   "you may preview data using \"--show data\"")
                resultList = []

        return resultList

    @abortOnError
    def __call__(self, cache, args):
        """Run the Task on a single target

        Strips out the process pool 'cache' argument.

        'args' are those arguments provided by the getTargetList method.

        Brings down the entire job if an exception is not caught (i.e., --doraise).
        """
        return TaskRunner.__call__(self, args)


class BatchParallelTask(BatchCmdLineTask):
    """Runs the BatchCmdLineTask in parallel

    Use this subclass of BatchCmdLineTask if you don't need to use the Pool
    directly, but just want to iterate over many objects (like a multi-node
    version of the '-j' command-line argument).
    """
    RunnerClass = BatchTaskRunner

    @classmethod
    def _makeArgumentParser(cls, *args, **kwargs):
        """Build an ArgumentParser

        Removes the batch-specific parts in order to delegate to the parent classes.
        """
        kwargs.pop("doBatch", False)
        kwargs.pop("add_help", False)
        return super(BatchCmdLineTask, cls)._makeArgumentParser(*args, **kwargs)

    @classmethod
    def parseAndRun(cls, *args, **kwargs):
        """Parse an argument list and run the command

        This is the entry point when we run in earnest, so start the process pool
        so that the worker nodes don't go any further.
        """
        pool = startPool()
        results = super(BatchParallelTask, cls).parseAndRun(*args, **kwargs)
        pool.exit()
        return results
