import os
import sys
import time
from functools import wraps

import mpi4py.MPI as mpi

from lsst.pipe.base import Struct

__all__ = ["Comm", "Tags", "startPool", "abortOnError",]

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
    """Debug logger singleton

    Disabled by default; to enable, do: 'Debugger().enabled = True'
    You can also redirect the output by changing the 'out' attribute.
    """
    _instance = None
    def __new__(cls):
        """Create singleton"""
        if not cls._instance:
            cls._instance = super(Debugger, cls).__new__(cls)
            cls._instance.enabled = False
            cls._instance.out = sys.stderr
        return cls._instance

    def log(self, source, msg, *args):
        """Log message

        The 'args' are only stringified if we're enabled.

        @param source: name of source
        @param msg: message to write
        @param args: additional outputs to append to message
        """
        if self.enabled:
            self.out.write("%s: %s" % (source, msg))
            for arg in args:
                self.out.write(" %s" % arg)
            self.out.write("\n")


class PoolNode(object):
    """Node in MPI process pool

    WARNING: You should not let a pool instance hang around at program
    termination, as the garbage collection behaves differently, and may
    cause a segmentation fault (signal 11).
    """
    def __init__(self, comm, root=0):
        self.comm = comm
        self.rank = self.comm.rank
        self.root = root
        self.size = self.comm.size
        self._cache = {}
        self._store = {}
        self.debugger = Debugger()

    def _getCache(self, index):
        """Retrieve cache for particular data

        The cache is updated with the contents of the store.
        """
        if index not in self._cache:
            self._cache[index] = Cache(self.comm)
        self._cache[index].__dict__.update(self._store)
        return self._cache[index]

    def log(self, msg, *args):
        """Log a debugging message"""
        self.debugger.log("Node %d" % self.rank, msg, *args)

    def _processQueue(self, func, passCache, queue, *args):
        """Process a queue of data

        The queue consists of a list of (index, data) tuples,
        where the index maps to the cache, and the data is
        passed to the 'func'.

        @param func: function for slaves to run
        @param passCache: True to pass a cache instance to func
        @param queue: List of (index,data) tuples to process
        @param args: Constant arguments
        @return list of results from applying 'func' to dataList
        """
        if passCache:
            return [func(self._getCache(i), data, *args) for i, data in queue]
        return [func(data, *args) for i, data in queue]

    def storeSet(self, name, value):
        """Set value in store"""
        self.log("storing", name, value)
        self._store[name] = value

    def storeDel(self, *nameList):
        """Delete value in store"""
        self.log("deleting from store", nameList)
        for name in nameList:
            del self._store[name]

    def storeClear(self):
        """Clear stored data"""
        self.log("clearing store")
        self._store = {}

    def clearCache(self):
        """Reset cache"""
        self.log("clearing cache")
        self._cache = {}


class PoolMaster(PoolNode):
    """Master node instance of MPI process pool

    Only the master node should instantiate this.

    WARNING: You should not let a pool instance hang around at program
    termination, as the garbage collection behaves differently, and may
    cause a segmentation fault (signal 11).
    """
    def __init__(self, *args, **kwargs):
        super(PoolMaster, self).__init__(*args, **kwargs)
        assert self.root == self.rank, "This is the master node"

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
        they can use to store data for subsequent executions (to ensure
        subsequent data is distributed in the same pattern as before,
        use the 'scatterToPrevious' method).  The cache also contains
        data that has been stored on the slaves.

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
            return self._processQueue(func, passCache, zip(range(num), dataList), *args)

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
    def scatterGatherNoBalance(self, func, passCache, dataList, *args):
        """Scatter work to slaves and gather the results

        Work is distributed statically, so there is no load balancing.

        Each slave applies the function to the data they're provided.
        The slaves may optionally be passed a cache instance, which
        they can store data in for subsequent executions (to ensure
        subsequent data is distributed in the same pattern as before,
        use the 'scatterToPrevious' method).  The cache also contains
        data that has been stored on the slaves.

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
            return self._processQueue(func, passCache, zip(range(num), dataList), *args)

        self.command("scatterGatherNoBalance")

        # Send function
        self.log("instruct")
        self.comm.broadcast((tags, func, args, passCache), root=self.rank)

        # Divide up the jobs
        # Try to give root the least to do, so it also has time to manage
        queue = zip(range(num), dataList) # index, data
        if num < self.size:
            distribution = [[queue[i]] for i in range(num)]
            distribution.insert(self.rank, [])
            for i in range(num, self.size - 1):
                distribution.append([])
        elif num % self.size == 0:
            numEach = num//self.size
            distribution = [queue[i*numEach:(i+1)*numEach] for i in range(self.size)]
        else:
            numEach = num//self.size
            distribution = [queue[i*numEach:(i+1)*numEach] for i in range(self.size)]
            for i in range(numEach*self.size, num):
                distribution[(self.rank + 1) % self.size].append
            distribution = list([] for i in range(self.size))
            for i, job in enumerate(queue, self.rank + 1):
                distribution[i % self.size].append(job)

        # Distribute jobs
        for source in range(self.size):
            if source == self.rank:
                continue
            self.log("send jobs to ", source)
            self.comm.send(distribution[source], source, tag=tags.work)

        # Execute our own jobs
        resultList = [None]*num

        def ingestResults(nodeResults, distList):
            for i, result in enumerate(nodeResults):
                index = distList[i][0]
                resultList[index] = result

        ourResults = self._processQueue(func, passCache, distribution[self.rank], *args)
        ingestResults(ourResults, distribution[self.rank])

        # Collect results
        pending = self.size - 1
        while pending > 0:
            status = mpi.Status()
            slaveResults = self.comm.recv(status=status, tag=tags.result, source=mpi.ANY_SOURCE)
            source = status.source
            self.log("gather from slave", source)
            ingestResults(slaveResults, distribution[source])
            pending -= 1

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
            return self._processQueue(func, True, zip(range(num), dataList), *args)

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
    def storeSet(self, name, value):
        """Store data on slave

        The data is made available to functions through the cache. The
        stored data differs from the cache in that it is identical for
        all operations, whereas the cache is specific to the data being
        operated upon.

        @param name: name of data to store
        @param value: value to store
        """
        super(PoolMaster, self).storeSet(name, value)
        self.command("storeSet")
        self.log("give data")
        self.comm.broadcast((name, value), root=self.rank)
        self.log("done")

    @abortOnError
    def storeDel(self, *nameList):
        """Delete stored data on slave"""
        super(PoolMaster, self).storeDel(*nameList)
        self.command("storeDel")
        self.log("tell names")
        self.comm.broadcast(nameList, root=self.rank)
        self.log("done")

    @abortOnError
    def storeClear(self):
        """Reset data store

        Slave data stores are reset.
        """
        super(PoolMaster, self).storeClear()
        self.command("storeClear")

    @abortOnError
    def clearCache(self):
        """Reset cache

        Slave caches are reset.
        """
        super(PoolMaster, self).clearCache()
        self.command("clearCache")

    def exit(self):
        """Command slaves to exit"""
        self.command("exit")


class PoolSlave(PoolNode):
    """Slave node instance of MPI process pool"""
    def log(self, msg, *args):
        """Log a debugging message"""
        self.debugger.log("Slave %d" % self.rank, msg, *args)

    @abortOnError
    def run(self):
        """Serve commands of master node

        Slave accepts commands, which are the names of methods to execute.
        This exits when a command returns a true value.
        """
        menu = dict((cmd, getattr(self, cmd)) for cmd in ("scatterGather", "scatterGatherNoBalance",
                                                          "scatterToPrevious", "storeSet", "storeDel",
                                                          "storeClear", "clearCache", "exit",))
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
            result = self._processQueue(func, passCache, [(index, data)], *args)[0]
            self.comm.send((index, result), self.root, tag=tags.result)
            self.log("waiting for job")
            job = self.comm.recv(tag=tags.work, source=self.root)

        self.log("done")

    def scatterGatherNoBalance(self):
        """Process bulk scattered data and return results"""
        self.log("waiting for instruction")
        tags, func, args, passCache = self.comm.broadcast(None, root=self.root)
        self.log("waiting for job")
        queue = self.comm.recv(tag=tags.work, source=self.root)

        resultList = []
        for index, data in queue:
            self.log("running job", index)
            result = self._processQueue(func, passCache, [(index, data)], *args)[0]
            resultList.append(result)

        self.comm.send(resultList, self.root, tag=tags.result)
        self.log("done")

    def scatterToPrevious(self):
        """Process the same scattered data processed previously"""
        self.log("waiting for instruction")
        tags, func, args = self.comm.broadcast(None, root=self.root)
        queue = self._cache.keys()
        index = queue.pop(0) if queue else -1
        self.log("request job", index)
        self.comm.gather(index, root=self.root)
        self.log("waiting for job")
        data = self.comm.scatter(root=self.root)

        while index >= 0:
            self.log("running job")
            result = func(self._getCache(index), data, *args)
            self.log("pending", queue)
            nextIndex = queue.pop(0) if queue else -1
            self.comm.send((index, result, nextIndex), self.root, tag=tags.result)
            index = nextIndex
            if index >= 0:
                data = self.comm.recv(tag=tags.work, source=self.root)

        self.log("done")

    def storeSet(self):
        """Set value in store"""
        name, value = self.comm.broadcast(None, root=self.root)
        super(PoolSlave, self).storeSet(name, value)

    def storeDel(self):
        """Delete value in store"""
        nameList = self.comm.broadcast(None, root=self.root)
        super(PoolSlave, self).storeDel(*nameList)

    def exit(self):
        """Allow exit from loop in 'run'"""
        return True

def startPool(comm=Comm(), root=0, killSlaves=True):
    """
    Returns a PoolMaster object for the master node.
    Slave nodes are run and then optionally killed.

    If you elect not to kill the slaves, note that they
    will emerge at the point this function was called,
    which is likely very different from the point the
    master is at, so it will likely be necessary to put
    in some rank dependent code (e.g., look at the 'rank'
    attribute of the returned pools).

    Note that the pool objects should be deleted (either
    by going out of scope or explicit 'del') before program
    termination to avoid a segmentation fault.

    @param comm: MPI communicator
    @param root: Rank of root/master node
    @param killSlaves: Kill slaves on completion?
        """
    if comm.rank == root:
        return PoolMaster(comm, root=root)
    pool = PoolSlave(comm, root=root)
    pool.run()
    if killSlaves:
        del pool # Required to prevent segmentation fault on exit
        exit()
    return pool
