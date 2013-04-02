import os
import sys
import signal
from functools import wraps
import pbasf2 as pbasf
from lsst.pipe.base import ArgumentParser, DataIdContainer, TaskRunner
from lsst.pipe.base.cmdLineTask import CmdLineTask

__all__ = ["thisNode", "abortOnError"]

os.umask(002)
def sigalrm_handler(signum, frame):
    sys.stderr.write('Signal handler called with signal %s\n' % (signum))
signal.signal(signal.SIGALRM, sigalrm_handler)

def thisNode():
    """Return a short string to identify the node: <name>:<pid>"""
    return "%s:%d" % (os.uname()[1], os.getpid())

def abortOnError(func):
    """Decorator to throw an MPI abort on an unhandled exception"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception, e:
            sys.stderr.write("%s on %s in %s: %s\n" % (type(e).__name__, thisNode(), func.__name__, e))
            import traceback
            traceback.print_exc(file=sys.stderr)
            import mpi4py.MPI as mpi
            mpi.COMM_WORLD.Abort(1)
    return wrapper



class DummyDataRef(object):
    """Quacks like a ButlerDataRef (where required), but is picklable."""
    def __init__(self, dataId):
        self.dataId = dataId
    def put(self, *args, **kwargs): pass # Because each node will attempt to write the config
    def get(self, *args, **kwargs): raise AssertionError("Nodes should not be reading using this dataRef")


class MpiDataIdContainer(DataIdContainer):
    """DataIdContainer that prevents all the MPI jobs from reading the registry

    Slaves receive the list of dataIds from the master, and set up a dummy
    list of dataRefs so that the Task.run method is called an
    appropriate number of times, which is the entire purpose.

    The dataRef that the slaves receive will not be useful (a DummyDataRef), so
    they will need to receive the dataRef over MPI from the master.
    """
    @abortOnError
    def makeDataRefList(self, namespace):
        # We don't want all the MPI jobs to go reading the registry at once
        comm = pbasf.Comm()
        rank = comm.rank
        root = 0
        if rank == root:
            super(MpiDataIdContainer, self).makeDataRefList(namespace)
            dummy = [DummyDataRef(dataRef.dataId) for dataRef in self.refList]
        else:
            dummy = None
        # Ensure there's the same entries, except the slaves can't go reading/writing except what they're told
        if comm.size > 1:
            dummy = pbasf.Broadcast(comm, dummy, root=root)
            if rank != root:
                self.refList = dummy


class MpiArgumentParser(ArgumentParser):
    """ArgumentParser that prevents all the MPI jobs from reading the registry"""
    def add_id_argument(self, *args, **kwargs):
        ContainerClass = kwargs.get("ContainerClass", MpiDataIdContainer)
        super(MpiArgumentParser, self).add_id_argument(*args, ContainerClass=ContainerClass, **kwargs)


class MpiTaskRunner(TaskRunner):
    """Get a butler into the Task scripts"""
    @staticmethod
    def getTargetList(parsedCmd, **kwargs):
        """MpiTask.run methods should receive a butler in the kwargs"""
        return TaskRunner.getTargetList(parsedCmd, butler=parsedCmd.butler, **kwargs)



class MpiTask(CmdLineTask):
    RunnerClass = MpiTaskRunner
    canMultiprocess = False

    def __init__(self, **kwargs):
        """Constructor.

        All nodes execute this method.
        """
        self.comm = pbasf.Comm()
        self.rank = self.comm.rank
        self.root = 0
        super(MpiTask, self).__init__(**kwargs)

    def writeConfig(self, *args, **kwargs):
        """Only master node should do this, to avoid race conditions.
        Config should be identical across nodes, so no harm in this.
        """
        if self.rank == self.root:
            super(MpiTask, self).writeConfig(*args, **kwargs)

    def writeSchemas(self, *args, **kwargs):
        """Only master node should do this, to avoid race conditions.
        Schema should be identical across nodes, so no harm in this.
        """
        if self.rank == self.root:
            super(MpiTask, self).writeSchemas(*args, **kwargs)
