from future import standard_library
standard_library.install_aliases()
from builtins import zip
from builtins import range
from past.builtins import basestring
from builtins import object
# MPI process pool
# Copyright 2013 Paul A. Price
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/copyleft/gpl.html>
#

import os
import sys
import time
import types
import copyreg
import threading
from functools import wraps, partial
from contextlib import contextmanager

import mpi4py.MPI as mpi

from lsst.pipe.base import Struct
from future.utils import with_metaclass

__all__ = ["Comm", "Pool", "startPool", "setBatchType", "getBatchType", "abortOnError", "NODE", ]

NODE = "%s:%d" % (os.uname()[1], os.getpid())  # Name of node


def unpickleInstanceMethod(obj, name):
    """Unpickle an instance method

    This has to be a named function rather than a lambda because
    pickle needs to find it.
    """
    return getattr(obj, name)


def pickleInstanceMethod(method):
    """Pickle an instance method

    The instance method is divided into the object and the
    method name.
    """
    obj = method.__self__
    name = method.__name__
    return unpickleInstanceMethod, (obj, name)

copyreg.pickle(types.MethodType, pickleInstanceMethod)


def unpickleFunction(moduleName, funcName):
    """Unpickle a function

    This has to be a named function rather than a lambda because
    pickle needs to find it.
    """
    import importlib
    module = importlib.import_module(moduleName)
    return getattr(module, funcName)


def pickleFunction(function):
    """Pickle a function

    This assumes that we can recreate the function object by grabbing
    it from the proper module.  This may be violated if the function
    is a lambda or in __main__.  In that case, I recommend recasting
    the function as an object with a __call__ method.

    Another problematic case may be a wrapped (e.g., decorated) method
    in a class: the 'method' is then a function, and recreating it is
    not as easy as we assume here.
    """
    moduleName = function.__module__
    funcName = function.__name__
    return unpickleFunction, (moduleName, funcName)

copyreg.pickle(types.FunctionType, pickleFunction)

try:
    _batchType
except NameError:
    _batchType = "unknown"

def getBatchType():
    """Return a string giving the type of batch system in use"""
    return _batchType

def setBatchType(batchType):
    """Return a string giving the type of batch system in use"""
    global _batchType
    _batchType = batchType

def abortOnError(func):
    """Function decorator to throw an MPI abort on an unhandled exception"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            sys.stderr.write("%s on %s in %s: %s\n" % (type(e).__name__, NODE, func.__name__, e))
            import traceback
            traceback.print_exc(file=sys.stderr)
            if getBatchType() is not None:
                mpi.COMM_WORLD.Abort(1)
            else:
                raise
    return wrapper


class PickleHolder(object):
    """Singleton to hold what's about to be pickled.

    We hold onto the object in case there's trouble pickling,
    so we can figure out what class in particular is causing
    the trouble.

    The held object is in the 'obj' attribute.

    Here we use the __new__-style singleton pattern, because
    we specifically want __init__ to be called each time.
    """

    _instance = None

    def __new__(cls, hold=None):
        if cls._instance is None:
            cls._instance = super(PickleHolder, cls).__new__(cls)
            cls._instance.__init__(hold)
            cls._instance.obj = None
        return cls._instance

    def __init__(self, hold=None):
        """Hold onto new object"""
        if hold is not None:
            self.obj = hold

    def __enter__(self):
        pass

    def __exit__(self, excType, excVal, tb):
        """Drop held object if there were no problems"""
        if excType is None:
            self.obj = None


def guessPickleObj():
    """Try to guess what's not pickling after an exception

    This tends to work if the problem is coming from the
    regular pickle module.  If it's coming from the bowels
    of mpi4py, there's not much that can be done.
    """
    import sys
    excType, excValue, tb = sys.exc_info()
    # Build a stack of traceback elements
    stack = []
    while tb:
        stack.append(tb)
        tb = tb.tb_next

    try:
        # This is the code version of a my way to find what's not pickling in pdb.
        # This should work if it's coming from the regular pickle module, and they
        # haven't changed the variable names since python 2.7.3.
        return stack[-2].tb_frame.f_locals["obj"]
    except:
        return None


@contextmanager
def pickleSniffer(abort=False):
    """Context manager to sniff out pickle problems

    If there's a pickle error, you're normally told what the problem
    class is.  However, all SWIG objects are reported as "SwigPyObject".
    In order to figure out which actual SWIG-ed class is causing
    problems, we need to go digging.

    Use like this:

        with pickleSniffer():
            someOperationInvolvingPickle()

    If 'abort' is True, will call MPI abort in the event of problems.
    """
    try:
        yield
    except Exception as e:
        if "SwigPyObject" not in str(e) or "pickle" not in str(e):
            raise
        import sys
        import traceback

        sys.stderr.write("Pickling error detected: %s\n" % e)
        traceback.print_exc(file=sys.stderr)
        obj = guessPickleObj()
        heldObj = PickleHolder().obj
        if obj is None and heldObj is not None:
            # Try to reproduce using what was being pickled using the regular pickle module,
            # so we've got a chance of figuring out what the problem is.
            import pickle
            try:
                pickle.dumps(heldObj)
                sys.stderr.write("Hmmm, that's strange: no problem with pickling held object?!?!\n")
            except Exception:
                obj = guessPickleObj()
        if obj is None:
            sys.stderr.write("Unable to determine class causing pickle problems.\n")
        else:
            sys.stderr.write("Object that could not be pickled: %s\n" % obj)
        if abort:
            if getBatchType() is not None:
                mpi.COMM_WORLD.Abort(1)
            else:
                sys.exit(1)

def catchPicklingError(func):
    """Function decorator to catch errors in pickling and print something useful"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        with pickleSniffer(True):
            return func(*args, **kwargs)
    return wrapper


class Comm(mpi.Intracomm):
    """Wrapper to mpi4py's MPI.Intracomm class to avoid busy-waiting.

    As suggested by Lisandro Dalcin at:
    * http://code.google.com/p/mpi4py/issues/detail?id=4 and
    * https://groups.google.com/forum/?fromgroups=#!topic/mpi4py/nArVuMXyyZI
    """

    def __new__(cls, comm=mpi.COMM_WORLD, recvSleep=0.1, barrierSleep=0.1):
        """!Construct an MPI.Comm wrapper

        @param cls             Class
        @param comm            MPI.Intracomm to wrap a duplicate of
        @param recvSleep       Sleep time (seconds) for recv()
        @param barrierSleep    Sleep time (seconds) for Barrier()
        """
        self = super(Comm, cls).__new__(cls, comm.Dup())
        self._barrierComm = None  # Duplicate communicator used for Barrier point-to-point checking
        self._recvSleep = recvSleep
        self._barrierSleep = barrierSleep
        return self

    def recv(self, obj=None, source=0, tag=0, status=None):
        """Version of comm.recv() that doesn't busy-wait"""
        sts = mpi.Status()
        while not self.Iprobe(source=source, tag=tag, status=sts):
            time.sleep(self._recvSleep)
        return super(Comm, self).recv(buf=obj, source=sts.source, tag=sts.tag, status=status)

    def send(self, obj=None, *args, **kwargs):
        with PickleHolder(obj):
            return super(Comm, self).send(obj, *args, **kwargs)

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
        with PickleHolder(value):
            return super(Comm, self).bcast(value, root=root)

    def scatter(self, dataList, root=0, tag=0):
        """Scatter data across the nodes

        The default version apparently pickles the entire 'dataList',
        which can cause errors if the pickle size grows over 2^31 bytes
        due to fundamental problems with pickle in python 2. Instead,
        we send the data to each slave node in turn; this reduces the
        pickle size.

        @param dataList  List of data to distribute; one per node
            (including root)
        @param root  Index of root node
        @param tag  Message tag (integer)
        @return  Data for this node
        """
        if self.Get_rank() == root:
            for rank, data in enumerate(dataList):
                if rank == root:
                    continue
                self.send(data, rank, tag=tag)
            return dataList[root]
        else:
            return self.recv(source=root, tag=tag)

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
    """An object to hold stuff between different scatter calls

    Includes a communicator by default, to allow intercommunication
    between nodes.
    """

    def __init__(self, comm):
        super(Cache, self).__init__(comm=comm)


class SingletonMeta(type):
    """!Metaclass to produce a singleton

    Doing a singleton mixin without a metaclass (via __new__) is
    annoying because the user has to name his __init__ something else
    (otherwise it's called every time, which undoes any changes).
    Using this metaclass, the class's __init__ is called exactly once.

    Because this is a metaclass, note that:
    * "self" here is the class
    * "__init__" is making the class (it's like the body of the
      class definition).
    * "__call__" is making an instance of the class (it's like
      "__new__" in the class).
    """

    def __init__(self, name, bases, dict_):
        super(SingletonMeta, self).__init__(name, bases, dict_)
        self._instance = None

    def __call__(self, *args, **kwargs):
        if self._instance is None:
            self._instance = super(SingletonMeta, self).__call__(*args, **kwargs)
        return self._instance


class Debugger(with_metaclass(SingletonMeta, object)):
    """Debug logger singleton

    Disabled by default; to enable, do: 'Debugger().enabled = True'
    You can also redirect the output by changing the 'out' attribute.
    """

    def __init__(self):
        self.enabled = False
        self.out = sys.stderr

    def log(self, source, msg, *args):
        """!Log message

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


class ReductionThread(threading.Thread):
    """Thread to do reduction of results

    "A thread?", you say. "What about the python GIL?"
    Well, because we 'sleep' when there's no immediate response from the
    slaves, that gives the thread a chance to fire; and threads are easier
    to manage (e.g., shared memory) than a process.
    """
    def __init__(self, reducer, initial=None, sleep=0.1):
        """!Constructor

        The 'reducer' should take two values and return a single
        (reduced) value.

        @param reducer  Function that does the reducing
        @param initial  Initial value for reduction, or None
        @param sleep  Time to sleep when there's nothing to do (sec)
        """
        threading.Thread.__init__(self, name="reducer")
        self._queue = []  # Queue of stuff to be reduced
        self._lock = threading.Lock()  # Lock for the queue
        self._reducer = reducer
        self._sleep = sleep
        self._result = initial  # Final result
        self._done = threading.Event()  # Signal that everything is done

    def _doReduce(self):
        """Do the actual work

        We pull the data out of the queue and release the lock before
        operating on it. This stops us from blocking the addition of
        new data to the queue.
        """
        with self._lock:
            queue = self._queue
            self._queue = []
        for data in queue:
            self._result = self._reducer(self._result, data) if self._result is not None else data

    def run(self):
        """Do the work

        Thread entry point, called by Thread.start
        """
        while True:
            self._doReduce()
            if self._done.wait(self._sleep):
                self._doReduce()
                return

    def add(self, data):
        """Add data to the queue to be reduced"""
        with self._lock:
            self._queue.append(data)

    def join(self):
        """Complete the thread

        Unlike Thread.join (which always returns 'None'), we return the result
        we calculated.
        """
        self._done.set()
        threading.Thread.join(self)
        return self._result


class PoolNode(with_metaclass(SingletonMeta, object)):
    """Node in MPI process pool

    WARNING: You should not let a pool instance hang around at program
    termination, as the garbage collection behaves differently, and may
    cause a segmentation fault (signal 11).
    """

    def __init__(self, comm=None, root=0):
        if comm is None:
            comm = Comm()
        self.comm = comm
        self.rank = self.comm.rank
        self.root = root
        self.size = self.comm.size
        self._cache = {}
        self._store = {}
        self.debugger = Debugger()
        self.node = NODE

    def _getCache(self, context, index):
        """Retrieve cache for particular data

        The cache is updated with the contents of the store.
        """
        if not context in self._cache:
            self._cache[context] = {}
        if not context in self._store:
            self._store[context] = {}
        cache = self._cache[context]
        store = self._store[context]
        if index not in cache:
            cache[index] = Cache(self.comm)
        cache[index].__dict__.update(store)
        return cache[index]

    def log(self, msg, *args):
        """Log a debugging message"""
        self.debugger.log("Node %d" % self.rank, msg, *args)

    def isMaster(self):
        return self.rank == self.root

    def _processQueue(self, context, func, queue, *args, **kwargs):
        """!Process a queue of data

        The queue consists of a list of (index, data) tuples,
        where the index maps to the cache, and the data is
        passed to the 'func'.

        The 'func' signature should be func(cache, data, *args, **kwargs)
        if 'context' is non-None; otherwise func(data, *args, **kwargs).

        @param context: Namespace for cache; None to not use cache
        @param func: function for slaves to run
        @param queue: List of (index,data) tuples to process
        @param args: Constant arguments
        @param kwargs: Keyword arguments
        @return list of results from applying 'func' to dataList
        """
        return self._reduceQueue(context, None, func, queue, *args, **kwargs)

    def _reduceQueue(self, context, reducer, func, queue, *args, **kwargs):
        """!Reduce a queue of data

        The queue consists of a list of (index, data) tuples,
        where the index maps to the cache, and the data is
        passed to the 'func', the output of which is reduced
        using the 'reducer' (if non-None).

        The 'func' signature should be func(cache, data, *args, **kwargs)
        if 'context' is non-None; otherwise func(data, *args, **kwargs).

        The 'reducer' signature should be reducer(old, new). If the 'reducer'
        is None, then we will return the full list of results

        @param context: Namespace for cache; None to not use cache
        @param reducer: function for master to run to reduce slave results; or None
        @param func: function for slaves to run
        @param queue: List of (index,data) tuples to process
        @param args: Constant arguments
        @param kwargs: Keyword arguments
        @return reduced result (if reducer is non-None) or list of results
            from applying 'func' to dataList
        """
        if context is not None:
            resultList = [func(self._getCache(context, i), data, *args, **kwargs) for i, data in queue]
        else:
            resultList = [func(data, *args, **kwargs) for i, data in queue]
        if reducer is None:
            return resultList
        if len(resultList) == 0:
            return None
        output = resultList.pop(0)
        for result in resultList:
            output = reducer(output, result)
        return output

    def storeSet(self, context, **kwargs):
        """Set values in store for a particular context"""
        self.log("storing", context, kwargs)
        if not context in self._store:
            self._store[context] = {}
        for name, value in kwargs.items():
            self._store[context][name] = value

    def storeDel(self, context, *nameList):
        """Delete value in store for a particular context"""
        self.log("deleting from store", context, nameList)
        if not context in self._store:
            raise KeyError("No such context: %s" % context)
        for name in nameList:
            del self._store[context][name]

    def storeClear(self, context):
        """Clear stored data for a particular context"""
        self.log("clearing store", context)
        if not context in self._store:
            raise KeyError("No such context: %s" % context)
        self._store[context] = {}

    def cacheClear(self, context):
        """Reset cache for a particular context"""
        self.log("clearing cache", context)
        if not context in self._cache:
            return
        self._cache[context] = {}

    def cacheList(self, context):
        """List contents of cache"""
        cache = self._cache[context] if context in self._cache else {}
        sys.stderr.write("Cache on %s (%s): %s\n" % (self.node, context, cache))

    def storeList(self, context):
        """List contents of store for a particular context"""
        if not context in self._store:
            raise KeyError("No such context: %s" % context)
        sys.stderr.write("Store on %s (%s): %s\n" % (self.node, context, self._store[context]))


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
        self.comm.broadcast(cmd, root=self.root)

    def map(self, context, func, dataList, *args, **kwargs):
        """!Scatter work to slaves and gather the results

        Work is distributed dynamically, so that slaves that finish
        quickly will receive more work.

        Each slave applies the function to the data they're provided.
        The slaves may optionally be passed a cache instance, which
        they can use to store data for subsequent executions (to ensure
        subsequent data is distributed in the same pattern as before,
        use the 'mapToPrevious' method).  The cache also contains
        data that has been stored on the slaves.

        The 'func' signature should be func(cache, data, *args, **kwargs)
        if 'context' is non-None; otherwise func(data, *args, **kwargs).

        @param context: Namespace for cache
        @param func: function for slaves to run; must be picklable
        @param dataList: List of data to distribute to slaves; must be picklable
        @param args: List of constant arguments
        @param kwargs: Dict of constant arguments
        @return list of results from applying 'func' to dataList
        """
        return self.reduce(context, None, func, dataList, *args, **kwargs)

    @abortOnError
    @catchPicklingError
    def reduce(self, context, reducer, func, dataList, *args, **kwargs):
        """!Scatter work to slaves and reduce the results

        Work is distributed dynamically, so that slaves that finish
        quickly will receive more work.

        Each slave applies the function to the data they're provided.
        The slaves may optionally be passed a cache instance, which
        they can use to store data for subsequent executions (to ensure
        subsequent data is distributed in the same pattern as before,
        use the 'mapToPrevious' method).  The cache also contains
        data that has been stored on the slaves.

        The 'func' signature should be func(cache, data, *args, **kwargs)
        if 'context' is non-None; otherwise func(data, *args, **kwargs).

        The 'reducer' signature should be reducer(old, new). If the 'reducer'
        is None, then we will return the full list of results

        @param context: Namespace for cache
        @param reducer: function for master to run to reduce slave results; or None
        @param func: function for slaves to run; must be picklable
        @param dataList: List of data to distribute to slaves; must be picklable
        @param args: List of constant arguments
        @param kwargs: Dict of constant arguments
        @return reduced result (if reducer is non-None) or list of results
            from applying 'func' to dataList
        """
        tags = Tags("request", "work")
        num = len(dataList)
        if self.size == 1 or num <= 1:
            return self._reduceQueue(context, reducer, func, list(zip(list(range(num)), dataList)),
                                     *args, **kwargs)
        if self.size == num:
            # We're shooting ourselves in the foot using dynamic distribution
            return self.reduceNoBalance(context, reducer, func, dataList, *args, **kwargs)

        self.command("reduce")

        # Send function
        self.log("instruct")
        self.comm.broadcast((tags, func, reducer, args, kwargs, context), root=self.root)

        # Parcel out first set of data
        queue = list(zip(range(num), dataList))  # index, data
        output = [None]*num if reducer is None else None
        initial = [None if i == self.rank else queue.pop(0) if queue else NoOp() for
                   i in range(self.size)]
        pending = min(num, self.size - 1)
        self.log("scatter initial jobs")
        self.comm.scatter(initial, root=self.rank)

        while queue or pending > 0:
            status = mpi.Status()
            report = self.comm.recv(status=status, tag=tags.request, source=mpi.ANY_SOURCE)
            source = status.source
            self.log("gather from slave", source)
            if reducer is None:
                index, result = report
                output[index] = result

            if queue:
                job = queue.pop(0)
                self.log("send job to slave", job[0], source)
            else:
                job = NoOp()
                pending -= 1
            self.comm.send(job, source, tag=tags.work)

        if reducer is not None:
            results = self.comm.gather(None, root=self.root)
            output = None
            for rank in range(self.size):
                if rank == self.root:
                    continue
                output = reducer(output, results[rank]) if output is not None else results[rank]

        self.log("done")
        return output

    def mapNoBalance(self, context, func, dataList, *args, **kwargs):
        """!Scatter work to slaves and gather the results

        Work is distributed statically, so there is no load balancing.

        Each slave applies the function to the data they're provided.
        The slaves may optionally be passed a cache instance, which
        they can store data in for subsequent executions (to ensure
        subsequent data is distributed in the same pattern as before,
        use the 'mapToPrevious' method).  The cache also contains
        data that has been stored on the slaves.

        The 'func' signature should be func(cache, data, *args, **kwargs)
        if 'context' is true; otherwise func(data, *args, **kwargs).

        @param context: Namespace for cache
        @param func: function for slaves to run; must be picklable
        @param dataList: List of data to distribute to slaves; must be picklable
        @param args: List of constant arguments
        @param kwargs: Dict of constant arguments
        @return list of results from applying 'func' to dataList
        """
        return self.reduceNoBalance(context, None, func, dataList, *args, **kwargs)

    @abortOnError
    @catchPicklingError
    def reduceNoBalance(self, context, reducer, func, dataList, *args, **kwargs):
        """!Scatter work to slaves and reduce the results

        Work is distributed statically, so there is no load balancing.

        Each slave applies the function to the data they're provided.
        The slaves may optionally be passed a cache instance, which
        they can store data in for subsequent executions (to ensure
        subsequent data is distributed in the same pattern as before,
        use the 'mapToPrevious' method).  The cache also contains
        data that has been stored on the slaves.

        The 'func' signature should be func(cache, data, *args, **kwargs)
        if 'context' is true; otherwise func(data, *args, **kwargs).

        The 'reducer' signature should be reducer(old, new). If the 'reducer'
        is None, then we will return the full list of results

        @param context: Namespace for cache
        @param reducer: function for master to run to reduce slave results; or None
        @param func: function for slaves to run; must be picklable
        @param dataList: List of data to distribute to slaves; must be picklable
        @param args: List of constant arguments
        @param kwargs: Dict of constant arguments
        @return reduced result (if reducer is non-None) or list of results
            from applying 'func' to dataList
        """
        tags = Tags("result", "work")
        num = len(dataList)
        if self.size == 1 or num <= 1:
            return self._reduceQueue(context, reducer, func, list(zip(range(num), dataList)), *args, **kwargs)

        self.command("mapNoBalance")

        # Send function
        self.log("instruct")
        self.comm.broadcast((tags, func, args, kwargs, context), root=self.root)

        # Divide up the jobs
        # Try to give root the least to do, so it also has time to manage
        queue = list(zip(range(num), dataList))  # index, data
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
        output = [None]*num if reducer is None else None

        def ingestResults(output, nodeResults, distList):
            if reducer is None:
                for i, result in enumerate(nodeResults):
                    index = distList[i][0]
                    output[index] = result
                return output
            if output is None:
                output = nodeResults.pop(0)
            for result in nodeResults:
                output = reducer(output, result)
            return output

        ourResults = self._processQueue(context, func, distribution[self.rank], *args, **kwargs)
        output = ingestResults(output, ourResults, distribution[self.rank])

        # Collect results
        pending = self.size - 1
        while pending > 0:
            status = mpi.Status()
            slaveResults = self.comm.recv(status=status, tag=tags.result, source=mpi.ANY_SOURCE)
            source = status.source
            self.log("gather from slave", source)
            output = ingestResults(output, slaveResults, distribution[source])
            pending -= 1

        self.log("done")
        return output

    def mapToPrevious(self, context, func, dataList, *args, **kwargs):
        """!Scatter work to the same target as before

        Work is distributed so that each slave handles the same
        indices in the dataList as when 'map' was called.
        This allows the right data to go to the right cache.

        It is assumed that the dataList is the same length as when it was
        passed to 'map'.

        The 'func' signature should be func(cache, data, *args, **kwargs).

        @param context: Namespace for cache
        @param func: function for slaves to run; must be picklable
        @param dataList: List of data to distribute to slaves; must be picklable
        @param args: List of constant arguments
        @param kwargs: Dict of constant arguments
        @return list of results from applying 'func' to dataList
        """
        return self.reduceToPrevious(context, None, func, dataList, *args, **kwargs)

    @abortOnError
    @catchPicklingError
    def reduceToPrevious(self, context, reducer, func, dataList, *args, **kwargs):
        """!Reduction where work goes to the same target as before

        Work is distributed so that each slave handles the same
        indices in the dataList as when 'map' was called.
        This allows the right data to go to the right cache.

        It is assumed that the dataList is the same length as when it was
        passed to 'map'.

        The 'func' signature should be func(cache, data, *args, **kwargs).

        The 'reducer' signature should be reducer(old, new). If the 'reducer'
        is None, then we will return the full list of results

        @param context: Namespace for cache
        @param reducer: function for master to run to reduce slave results; or None
        @param func: function for slaves to run; must be picklable
        @param dataList: List of data to distribute to slaves; must be picklable
        @param args: List of constant arguments
        @param kwargs: Dict of constant arguments
        @return reduced result (if reducer is non-None) or list of results
            from applying 'func' to dataList
        """
        if context is None:
            raise ValueError("context must be set to map to same nodes as previous context")
        tags = Tags("result", "work")
        num = len(dataList)
        if self.size == 1 or num <= 1:
            # Can do everything here
            return self._reduceQueue(context, reducer, func, list(zip(range(num), dataList)), *args, **kwargs)
        if self.size == num:
            # We're shooting ourselves in the foot using dynamic distribution
            return self.reduceNoBalance(context, func, dataList, *args, **kwargs)

        self.command("mapToPrevious")

        # Send function
        self.log("instruct")
        self.comm.broadcast((tags, func, args, kwargs, context), root=self.root)

        requestList = self.comm.gather(None, root=self.root)
        self.log("listen", requestList)
        initial = [dataList[index] if (index is not None and index >= 0) else None for index in requestList]
        self.log("scatter jobs", initial)
        self.comm.scatter(initial, root=self.root)
        pending = min(num, self.size - 1)

        if reducer is None:
            output = [None]*num
        else:
            thread = ReductionThread(reducer)
            thread.start()

        while pending > 0:
            status = mpi.Status()
            index, result, nextIndex = self.comm.recv(status=status, tag=tags.result, source=mpi.ANY_SOURCE)
            source = status.source
            self.log("gather from slave", source)
            if reducer is None:
                output[index] = result
            else:
                thread.add(result)

            if nextIndex >= 0:
                job = dataList[nextIndex]
                self.log("send job to slave", source)
                self.comm.send(job, source, tag=tags.work)
            else:
                pending -= 1

            self.log("waiting on", pending)

        if reducer is not None:
            output = thread.join()

        self.log("done")
        return output

    @abortOnError
    @catchPicklingError
    def storeSet(self, context, **kwargs):
        """!Store data on slave for a particular context

        The data is made available to functions through the cache. The
        stored data differs from the cache in that it is identical for
        all operations, whereas the cache is specific to the data being
        operated upon.

        @param context: namespace for store
        @param kwargs: dict of name=value pairs
        """
        super(PoolMaster, self).storeSet(context, **kwargs)
        self.command("storeSet")
        self.log("give data")
        self.comm.broadcast((context, kwargs), root=self.root)
        self.log("done")

    @abortOnError
    def storeDel(self, context, *nameList):
        """Delete stored data on slave for a particular context"""
        super(PoolMaster, self).storeDel(context, *nameList)
        self.command("storeDel")
        self.log("tell names")
        self.comm.broadcast((context, nameList), root=self.root)
        self.log("done")

    @abortOnError
    def storeClear(self, context):
        """Reset data store for a particular context on master and slaves"""
        super(PoolMaster, self).storeClear(context)
        self.command("storeClear")
        self.comm.broadcast(context, root=self.root)

    @abortOnError
    def cacheClear(self, context):
        """Reset cache for a particular context on master and slaves"""
        super(PoolMaster, self).cacheClear(context)
        self.command("cacheClear")
        self.comm.broadcast(context, root=self.root)

    @abortOnError
    def cacheList(self, context):
        """List cache contents for a particular context on master and slaves"""
        super(PoolMaster, self).cacheList(context)
        self.command("cacheList")
        self.comm.broadcast(context, root=self.root)

    @abortOnError
    def storeList(self, context):
        """List store contents for a particular context on master and slaves"""
        super(PoolMaster, self).storeList(context)
        self.command("storeList")
        self.comm.broadcast(context, root=self.root)

    def exit(self):
        """Command slaves to exit"""
        self.command("exit")


class PoolSlave(PoolNode):
    """Slave node instance of MPI process pool"""

    def log(self, msg, *args):
        """Log a debugging message"""
        assert self.rank != self.root, "This is not the master node."
        self.debugger.log("Slave %d" % self.rank, msg, *args)

    @abortOnError
    def run(self):
        """Serve commands of master node

        Slave accepts commands, which are the names of methods to execute.
        This exits when a command returns a true value.
        """
        menu = dict((cmd, getattr(self, cmd)) for cmd in ("reduce", "mapNoBalance", "mapToPrevious",
                                                          "storeSet", "storeDel", "storeClear", "storeList",
                                                          "cacheList", "cacheClear", "exit",))
        self.log("waiting for command from", self.root)
        command = self.comm.broadcast(None, root=self.root)
        self.log("command", command)
        while not menu[command]():
            self.log("waiting for command from", self.root)
            command = self.comm.broadcast(None, root=self.root)
            self.log("command", command)
        self.log("exiting")

    @catchPicklingError
    def reduce(self):
        """Reduce scattered data and return results"""
        self.log("waiting for instruction")
        tags, func, reducer, args, kwargs, context = self.comm.broadcast(None, root=self.root)
        self.log("waiting for job")
        job = self.comm.scatter(None, root=self.root)

        out = None  # Reduction result
        while not isinstance(job, NoOp):
            index, data = job
            self.log("running job")
            result = self._processQueue(context, func, [(index, data)], *args, **kwargs)[0]
            if reducer is None:
                report = (index, result)
            else:
                report = None
                out = reducer(out, result) if out is not None else result
            self.comm.send(report, self.root, tag=tags.request)
            self.log("waiting for job")
            job = self.comm.recv(tag=tags.work, source=self.root)

        if reducer is not None:
            self.comm.gather(out, root=self.root)
        self.log("done")

    @catchPicklingError
    def mapNoBalance(self):
        """Process bulk scattered data and return results"""
        self.log("waiting for instruction")
        tags, func, args, kwargs, context = self.comm.broadcast(None, root=self.root)
        self.log("waiting for job")
        queue = self.comm.recv(tag=tags.work, source=self.root)

        resultList = []
        for index, data in queue:
            self.log("running job", index)
            result = self._processQueue(context, func, [(index, data)], *args, **kwargs)[0]
            resultList.append(result)

        self.comm.send(resultList, self.root, tag=tags.result)
        self.log("done")

    @catchPicklingError
    def mapToPrevious(self):
        """Process the same scattered data processed previously"""
        self.log("waiting for instruction")
        tags, func, args, kwargs, context = self.comm.broadcast(None, root=self.root)
        queue = list(self._cache[context].keys()) if context in self._cache else None
        index = queue.pop(0) if queue else -1
        self.log("request job", index)
        self.comm.gather(index, root=self.root)
        self.log("waiting for job")
        data = self.comm.scatter(None, root=self.root)

        while index >= 0:
            self.log("running job")
            result = func(self._getCache(context, index), data, *args, **kwargs)
            self.log("pending", queue)
            nextIndex = queue.pop(0) if queue else -1
            self.comm.send((index, result, nextIndex), self.root, tag=tags.result)
            index = nextIndex
            if index >= 0:
                data = self.comm.recv(tag=tags.work, source=self.root)

        self.log("done")

    def storeSet(self):
        """Set value in store"""
        context, kwargs = self.comm.broadcast(None, root=self.root)
        super(PoolSlave, self).storeSet(context, **kwargs)

    def storeDel(self):
        """Delete value in store"""
        context, nameList = self.comm.broadcast(None, root=self.root)
        super(PoolSlave, self).storeDel(context, *nameList)

    def storeClear(self):
        """Reset data store"""
        context = self.comm.broadcast(None, root=self.root)
        super(PoolSlave, self).storeClear(context)

    def cacheClear(self):
        """Reset cache"""
        context = self.comm.broadcast(None, root=self.root)
        super(PoolSlave, self).cacheClear(context)

    def cacheList(self):
        """List cache contents"""
        context = self.comm.broadcast(None, root=self.root)
        super(PoolSlave, self).cacheList(context)

    def storeList(self):
        """List store contents"""
        context = self.comm.broadcast(None, root=self.root)
        super(PoolSlave, self).storeList(context)

    def exit(self):
        """Allow exit from loop in 'run'"""
        return True


class PoolWrapperMeta(type):
    """Metaclass for PoolWrapper to add methods pointing to PoolMaster

    The 'context' is automatically supplied to these methods as the first argument.
    """

    def __call__(self, context="default"):
        instance = super(PoolWrapperMeta, self).__call__(context)
        pool = PoolMaster()
        for name in ("map", "mapNoBalance", "mapToPrevious",
                     "reduce", "reduceNoBalance", "reduceToPrevious",
                     "storeSet", "storeDel", "storeClear", "storeList",
                     "cacheList", "cacheClear",):
            setattr(instance, name, partial(getattr(pool, name), context))
        return instance


class PoolWrapper(with_metaclass(PoolWrapperMeta, object)):
    """Wrap PoolMaster to automatically provide context"""

    def __init__(self, context="default"):
        self._pool = PoolMaster._instance
        self._context = context

    def __getattr__(self, name):
        return getattr(self._pool, name)


class Pool(PoolWrapper):     # Just gives PoolWrapper a nicer name for the user
    """Process Pool

    Use this class to automatically provide 'context' to
    the PoolMaster class.  If you want to call functions
    that don't take a 'cache' object, use the PoolMaster
    class directly, and specify context=None.
    """
    pass


def startPool(comm=None, root=0, killSlaves=True):
    """!Start a process pool.

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
    if comm is None:
        comm = Comm()
    if comm.rank == root:
        return PoolMaster(comm, root=root)
    slave = PoolSlave(comm, root=root)
    slave.run()
    if killSlaves:
        del slave  # Required to prevent segmentation fault on exit
        sys.exit()
    return slave
