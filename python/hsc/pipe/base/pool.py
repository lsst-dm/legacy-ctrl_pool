import os
import sys
import time
from functools import wraps

import mpi4py.MPI as mpi

from lsst.pipe.base import Struct

__all__ = ["Comm", "Tags", "Pool", "abortOnError",]

def abortOnError(func):
    """Decorator to throw an MPI abort on an unhandled exception"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception, e:
            sys.stderr.write("%s on %s:%s in %s: %s\n" % (type(e).__name__, os.uname()[1], os.getpid(),
                                                          func.__name__, e))
            import traceback
            traceback.print_exc(file=sys.stderr)
            mpi.COMM_WORLD.Abort(1)
    return wrapper

class Comm(mpi.Intracomm):
    """Wrapper to mpi4py's MPI.Intracomm class to avoid busy-waiting.

    As suggested by Lisandro Dalcin at:
    * http://code.google.com/p/mpi4py/issues/detail?id=4 and
    * https://groups.google.com/forum/?fromgroups=#!topic/mpi4py/nArVuMXyyZI
    """
    def __new__(cls, comm=mpi.COMM_WORLD, recvSleep=1.0, barrierSleep=1.0):
        """Construct an MPI.Comm wrapper

        @param comm            MPI.Intracomm to wrap a duplicate of
        @param recvSleep       Sleep time (seconds) for recv()
        @param barrierSleep    Sleep time (seconds) for Barrier()
        """
        self = super(Comm, cls).__new__(cls, comm.Dup())
        self._barrierComm = None # Duplicate communicator used for Barrier point-to-point checking
        self._recvSleep = recvSleep
        self._barrierSleep = barrierSleep
        return self

    def recv(self, obj=None, source=0, tag=0, status=None):
        """Version of comm.recv() that doesn't busy-wait"""
        sts = mpi.Status()
        while not self.Iprobe(source=source, tag=tag, status=sts):
            time.sleep(self._recvSleep)
        return super(Comm, self).recv(obj=obj, source=sts.source, tag=sts.tag, status=status)

    def _checkBarrierComm(self):
        """Ensure the duplicate communicator is available"""
        if self._barrierComm is None:
            self._barrierComm = self.Dup()

    def Barrier(self, tag=0):
        """Version of comm.Barrier() that doesn't busy-wait

        A duplicate communicator is used so as not to interfere with the user's own communications.
        """
        self._checkBarrierComm()
        size = self._barrierComm.Get_size()
        if size == 1:
            return
        rank = self._barrierComm.Get_rank()
        mask = 1
        while mask < size:
            dst = (rank + mask) % size
            src = (rank - mask + size) % size
            req = self._barrierComm.isend(None, dst, tag)
            while not self._barrierComm.Iprobe(src, tag):
                time.sleep(self._barrierSleep)
            self._barrierComm.recv(None, src, tag)
            req.Wait()
            mask <<= 1

    def broadcast(self, value, root=0):
        self.Barrier()
        return super(Comm, self).bcast(value, root=root)

    def Free(self):
       if self._barrierComm is not None:
           self._barrierComm.Free()
       super(Comm, self).Free()


class NoOp(object):
    """Object to signal no operation"""
    pass

class Tags(object):
    """Provides tag numbers by symbolic name in attributes"""
    def __init__(self, *nameList):
        self._nameList = nameList
        for i, name in enumerate(nameList, 1):
            setattr(self, name, i)
    def __repr__(self):
        return self.__class__.__name__ + repr(self._nameList)
    def __reduce__(self):
        return self.__class__, tuple(self._nameList)

class Cache(Struct):
    """An object to hold stuff between different scatter calls"""
    def __init__(self, comm):
        super(Cache, self).__init__(comm=comm)

class Debugger(object):
    _instance = None
    def __new__(cls):
        if not cls._instance:
            cls._instance = super(Debugger, cls).__new__(cls)
            cls._instance.enabled = False
            cls._instance.out = sys.stderr
        return cls._instance

    def log(self, source, msg, *args):
        if self.enabled:
            self.out.write("%s: %s" % (source, msg))
            for arg in args:
                self.out.write(" %s" % arg)
            self.out.write("\n")

class PoolMaster(object):
    """Master node instance of MPI process pool

    WARNING: You should not let a pool instance hang around at program
    termination, as the garbage collection behaves differently, and may
    cause a segmentation fault (signal 11).
    """
    def __new__(cls, root=0):
        """Create pool

        Returns a PoolMaster object only for the master node.
        Slave nodes are run and then killed.

        @param comm: MPI communicator
        @param root: Rank of root/master node
        """
        comm = Comm()
        if comm.rank == root:
            pool = super(PoolMaster, cls).__new__(cls, comm, root)
            pool.comm = comm
            return pool
        pool = PoolSlave(comm, root=root)
        pool.run()
        del pool # Required to prevent segmentation fault on exit
        exit()

    def __init__(self, root=0):
        """Constructor

        Only the master node should instantiate this.

        @param comm: MPI communicator
        @param root: Rank of root/master node (this one)
        """
        assert self.comm, "Enforced by __new__"
        self.rank = self.comm.rank
        assert self.rank == root, "Enforced by __new__"
        self.size = self.comm.size
        self.cache = {}
        self.debugger = Debugger()

    def __del__(self):
        """Ensure slaves exit when we're done"""
        self.exit()

    def log(self, msg, *args):
        """Log a debugging message"""
        self.debugger.log("Master", msg, *args)

    def command(self, cmd):
        """Send command to slaves

        A command is the name of the PoolSlave method they should run.
        """
        self.log("command", cmd)
        self.comm.broadcast(cmd, root=self.rank)

    @abortOnError
    def scatterGather(self, func, passCache, dataList, *args):
        """Scatter work to slaves and gather the results

        Work is distributed dynamically, so that slaves that finish
        quickly will receive more work.

        Each slave applies the function to the data they're provided.
        The slaves may optionally be passed a cache instance, which
        they can store data in for subsequent executions (to ensure
        subsequent data is distributed in the same pattern as before,
        use the 'scatterToPrevious' method).  The cache can be cleared
        by calling the 'clearCache' method.

        The 'func' signature should be func(cache, data, *args) if
        'passCache' is true; otherwise func(data, *args).

        @param func: function for slaves to run; must be picklable
        @param passCache: True to pass a cache instance to func
        @param dataList: List of data to distribute to slaves; must be picklable
        @param args: Constant arguments
        @return list of results from applying 'func' to dataList
        """
        tags = Tags("result", "work")
        num = len(dataList)
        if self.size == 1 or num <= 1:
            # Can do everything here
            if passCache:
                self.cache.update((i, Cache(self.comm)) for i in range(num) if i not in self.cache)
                return [func(self.cache[i], data, *args) for i, data in enumerate(dataList)]
            else:
                return [func(data, *args) for data in dataList]

        self.command("scatterGather")

        # Send function
        self.log("instruct")
        self.comm.broadcast((tags, func, args, passCache), root=self.rank)

        # Parcel out first set of data
        queue = zip(range(num), dataList) # index, data
        resultList = [None]*num
        initial = [None if i == self.rank else queue.pop(0) if queue else NoOp() for
                   i in range(self.size)]
        pending = min(num, self.size - 1)
        self.log("scatter initial jobs")
        self.comm.scatter(initial, root=self.rank)

        while queue or pending > 0:
            status = mpi.Status()
            index, result = self.comm.recv(status=status, tag=tags.result, source=mpi.ANY_SOURCE)
            source = status.source
            self.log("gather from slave", source)
            resultList[index] = result

            if queue:
                job = queue.pop(0)
                self.log("send job to slave", source)
            else:
                job = NoOp()
                pending -= 1
            self.comm.send(job, source, tag=tags.work)

        self.log("done")
        return resultList

    @abortOnError
    def scatterToPrevious(self, func, dataList, *args):
        """Scatter work to the same target as before

        Work is distributed so that each slave handles the same
        indices in the dataList as when 'scatterGather' was called.
        This allows the right data to go to the right cache.

        The 'func' signature should be func(cache, data, *args).

        @param func: function for slaves to run; must be picklable
        @param dataList: List of data to distribute to slaves; must be picklable
        @param args: Constant arguments
        @return list of results from applying 'func' to dataList
        """
        tags = Tags("result", "work")
        num = len(dataList)
        if self.size == 1 or num <= 1:
            # Can do everything here
            return [func(self.cache[i], data, *args) for i, data in enumerate(dataList)]

        self.command("scatterToPrevious")

        # Send function
        self.log("instruct")
        self.comm.broadcast((tags, func, args), root=self.rank)

        requestList = self.comm.gather(root=self.rank)
        self.log("listen", requestList)
        initial = [dataList[index] if index >= 0 else None for index in requestList]
        self.log("scatter jobs", initial)
        self.comm.scatter(initial, root=self.rank)
        pending = min(num, self.size - 1)

        resultList = [None]*num
        while pending > 0:
            status = mpi.Status()
            index, result, nextIndex = self.comm.recv(status=status, tag=tags.result, source=mpi.ANY_SOURCE)
            source = status.source
            self.log("gather from slave", source)
            resultList[index] = result

            if nextIndex >= 0:
                job = dataList[nextIndex]
                self.log("send job to slave", source)
                self.comm.send(job, source, tag=tags.work)
            else:
                pending -= 1

            self.log("waiting on", pending)

        self.log("done")
        return resultList

    @abortOnError
    def clearCache(self):
        """Reset cache

        Slave caches are reset.
        """
        self.cache = {}
        self.command("clearCache")

    def exit(self):
        """Command slaves to exit"""
        self.command("exit")

class PoolSlave(object):
    """Slave node instance of MPI process pool"""
    def __init__(self, comm, root=0):
        """Constructor

        Only slave nodes should instantiate this.

        @param comm: MPI communicator
        @param root: Rank of root/master node
        """
        self.comm = comm
        self.rank = comm.rank
        self.root = root
        self.size = comm.size
        self.cache = {}
        self.debugger = Debugger()

    def log(self, msg, *args):
        """Log a debugging message"""
        self.debugger.log("Slave %d" % self.rank, msg, *args)

    @abortOnError
    def run(self):
        """Serve commands of master node

        Slave accepts commands, which are the names of methods to execute.
        This exits when a command returns a true value.
        """
        menu = dict((cmd, getattr(self, cmd)) for cmd in ("scatterGather", "scatterToPrevious", 
                                                          "clearCache", "exit",))
        self.log("waiting for command from", self.root)
        command = self.comm.broadcast(None, root=self.root)
        self.log("command", command)
        while not menu[command]():
            self.log("waiting for command from", self.root)
            command = self.comm.broadcast(None, root=self.root)
            self.log("command", command)
        self.log("exiting")

    def scatterGather(self):
        """Process scattered data and return results"""
        self.log("waiting for instruction")
        tags, func, args, passCache = self.comm.broadcast(None, root=self.root)
        self.log("waiting for job")
        job = self.comm.scatter(root=self.root)

        while not isinstance(job, NoOp):
            index, data = job
            self.log("running job")
            if passCache:
                if index not in self.cache:
                    self.cache[index] = Cache(self.comm)
                result = func(self.cache[index], data, *args)
            else:
                result = func(data, *args)
            self.comm.send((index, result), self.root, tag=tags.result)
            self.log("waiting for job")
            job = self.comm.recv(tag=tags.work, source=self.root)

        self.log("done")

    def scatterToPrevious(self):
        """Process the same scattered data processed previously"""
        self.log("waiting for instruction")
        tags, func, args = self.comm.broadcast(None, root=self.root)
        queue = self.cache.keys()
        index = queue.pop(0) if queue else -1
        self.log("request job", index)
        self.comm.gather(index, root=self.root)
        self.log("waiting for job")
        data = self.comm.scatter(root=self.root)

        while index >= 0:
            self.log("running job")
            result = func(self.cache[index], data, *args)
            self.log("pending", queue)
            nextIndex = queue.pop(0) if queue else -1
            self.comm.send((index, result, nextIndex), self.root, tag=tags.result)
            index = nextIndex
            if index >= 0:
                data = self.comm.recv(tag=tags.work, source=self.root)

        self.log("done")

    def clearCache(self):
        """Reset cache"""
        self.cache = {}

    def exit(self):
        """Allow exit from loop in 'run'"""
        return True

Pool = PoolMaster

