import os
import sys
import signal
from functools import wraps
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

def getComm():
    """Return an MPI communicator

    We use a PBASF2 communicator, because that disables busy-waiting.
    """
    import pbasf2
    return pbasf2.Comm()

class DummyDataRef(object):
    """Quacks like a ButlerDataRef (where required), but is picklable."""
    def __init__(self, dataId):
        self.dataId = dataId
    def put(self, *args, **kwargs): pass # Because each node will attempt to write the config
    def get(self, *args, **kwargs): raise AssertionError("Nodes should not be reading using this dataRef")

class MpiArgumentParser(ArgumentParser):
    """ArgumentParser that prevents all the MPI jobs from reading the registry"""
    def add_id_argument(self, *args, **kwargs):
        ContainerClass = kwargs.get("ContainerClass", DataIdContainer)
        rootOnly = kwargs.pop("rootOnly", True) # Are data references to be only defined on the root?
        if rootOnly:
            class MpiDataIdContainer(ContainerClass):
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
                    comm = getComm()
                    rank = comm.rank
                    root = 0
                    if rank == root:
                        super(MpiDataIdContainer, self).makeDataRefList(namespace)
                        dummy = [DummyDataRef(dataRef.dataId) for dataRef in self.refList]
                    else:
                        dummy = None
                    # Ensure there's the same number of entries, however the slaves can't
                    # go reading/writing except what they're told
                    if comm.size > 1:
                        import pbasf2
                        dummy = pbasf2.Broadcast(comm, dummy, root=root)
                        if rank != root:
                    self.refList = dummy
            ContainerClass = MpiDataIdContainer
        super(MpiArgumentParser, self).add_id_argument(*args, ContainerClass=ContainerClass, **kwargs)


class MpiTask(CmdLineTask):
    canMultiprocess = False

    def __init__(self, **kwargs):
        """Constructor.

        All nodes execute this method.
        """
        self.root = 0
        super(MpiTask, self).__init__(**kwargs)

    @property
    def comm(self):
        return getComm()

    @property
    def rank(self):
        return self.comm.rank

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

class MpiSimpleMapFunc(object):
    """Replacement for map builtin function, using MPI

    This implements a scatter-gather, with simple distribution of jobs
    (i.e., no attempts at load balancing).  Note that the root node also
    works (c.f. PBASF's ScatterJob) before collecting the outputs and
    returning them.

    The inputs (function and argument list) should be defined on all
    nodes (if not, use MPI to Broadcast them).  The result is only valid
    on the root node; other nodes return a list of the correct size, but
    all entries are None (if this is not suitable, use MPI to Broadcast).

    This is constructed with mpi4py in mind; other MPI wrappers may
    require minor modifications for slightly different APIs.
    """
    def __init__(self, comm, root=0):
        """Constructor

        @param comm: MPI communicator, e.g., mpi4py.COMM_WORLD
        @param root: Rank of root node
        """
        self.comm = comm
        self.size = self.comm.size
        self.rank = self.comm.rank
        self.root = root

    def doProcess(self, index, rank):
        """Process data with this index on this node (i.e., with this rank)?

        This is the key to the division of labour among the nodes.
        """
        return index % self.size == rank

    def scatter(self, func, argList):
        """Scatter the processing among the nodes

        All nodes execute this function, and the inputs should be defined
        on all nodes.  I.e., all nodes know what to do, but only do what
        they themselves need to do.  If only the root node has the inputs,
        then it should Broadcast them before calling.

        @param func: Function to apply to the arguments
        @param argList: List of arguments to the function
        @return List of results (with None for entries this node didn't process)
        """
        return [func(arg) if self.doProcess(i, self.rank) else None for i, arg in enumerate(argList)]

    def gather(self, resultList):
        """Gather the processing results

        All nodes execute this function, but their route through it differs.
        The root node collects the results from all the other nodes and returns
        the list.  The other nodes return a list of the correct size, but all
        entries are None.

        @param resultList: List of results from scatter processing
        @return Gathered list of results (root) or fake list of results (worker)
        """
        if self.rank == self.root:
            pending = set(range(self.size))
            pending.remove(self.root)
            while len(pending) > 0:
                node, outputList = self.comm.recv(status=mpi.Status(), tag=1, source=mpi.ANY_SOURCE)
                assert len(resultList) == len(outputList)
                resultList = [out if self.doProcess(i, node) else res for i,(out,res) in
                              enumerate(zip(outputList, resultList))]
                pending.remove(node)
            return resultList
        else:
            self.comm.send(dest=self.root, obj=(self.rank, output), tag=1)
            return [None]*len(resultList)

    def __call__(self, func, argList):
        """Apply function to each of the arguments, similar to the builtin 'map' function

        The inputs should be defined on all nodes.
        The result will only be defined on the root node.
        """
        results = self.scatter(func, argList)
        return self.gather(results)

class MpiQueuedMapFunc(object):
    def __init__(self, comm, root=0):
        """Constructor

        @param comm: MPI communicator, e.g., mpi4py.COMM_WORLD
        @param root: Rank of root node
        """
        self.comm = comm
        self.root = root

    def __call__(self, func, argList):
        """Apply function to each of the arguments, similar to the builtin 'map' function

        The inputs should be defined on all nodes.
        The result will only be defined on the root node.

        The root node serves as a master and does not do any actual work,
        but this allows the load to be balanced over the remaining nodes.
        """
        import pbasf2
        return pbasf2.ScatterJob(self.comm, func, argList, root=self.root)


class MpiMultiplexTaskRunner(TaskRunner):
    def run(self, parsedCmd):
        """Run the task on all targets.

        The task is run under MPI if numProcesses > 1; otherwise processing is serial.

        @return a list of results returned by __call__; see __call__ for details.
        """
        if self.numProcesses > 1:
            mapFunc = MpiQueuedMapFunc(getComm())
        else:
            mapFunc = map

        if self.precall(parsedCmd):
            resultList = mapFunc(self, self.getTargetList(parsedCmd))
        else:
            resultList = None

        return resultList

def wrapTask(TaskClass, globalsDict=None):
    """Wrap an ordinary CmdLineTask to use MPI for multiplexing

    @param TaskClass: The class to wrap
    @param globalsDict: The module globals; you probably just want to call globals()
    @param wrapped class
    """
    class MpiWrapper(TaskClass):
        RunnerClass = MpiMultiplexTaskRunner
    name = "Mpi" + TaskClass.__name__
    MpiWrapper.__name__ = name
    if globalsDict is not None:
        globalsDict[name] = MpiWrapper
    return MpiWrapper

