import sys
import time
from collections import OrderedDict
import mpi4py.MPI as mpi

__all__ = ["Comm", "Tags", "Pool"]


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


class PoolMaster(object):
    def __init__(self, comm):
        self.comm = comm
        self.rank = comm.rank
        self.size = comm.size
        self.cache = {}

    def __del__(self):
        self.exit()

    def command(self, cmd):
        self.comm.broadcast(cmd, root=self.rank)

    def scatterGather(self, func, passPool, dataList, *args):
        tags = Tags("result", "work")
        num = len(dataList)
        if self.size == 1 or num <= 1:
            # Can do everything here
            if passPool:
                return [func(self, data, *args) for data in dataList]
            else:
                return [func(data, *args) for data in dataList]

        print "Master commands"
        self.command("scatterGather")

        # Send function
        print "Master instructs"
        self.comm.broadcast((tags, func, args, passPool), root=self.rank)

        # Parcel out first set of data
        queue = zip(range(num), dataList) # index, data
        print "Master queue: %s" % queue
        resultList = [None]*num
        initial = [None if i == self.rank else queue.pop(0) if queue else NoOp() for
                   i in range(self.size)]
        pending = min(num, self.size - 1)
        print "Master scatters %d jobs: %s" % (len(initial), initial)
        self.comm.scatter(initial, root=self.rank)

        while queue or pending > 0:
            status = mpi.Status()
            index, result = self.comm.recv(status=status, tag=tags.result, source=mpi.ANY_SOURCE)
            source = status.source
            print "Master gathers from slave %d" % source
            resultList[index] = result

            if queue:
                job = queue.pop(0)
                print "Master sends job to slave %d" % source
            else:
                job = NoOp()
                pending -= 1
            self.comm.send(job, source, tag=tags.work)

        print "Master done"
        return resultList

    def clearCache(self):
        self.cache = {}
        self.command("clearCache")

    def exit(self):
        self.command("exit")

class PoolSlave(object):
    def __init__(self, comm, root=0):
        self.comm = comm
        self.rank = comm.rank
        self.root = root
        self.size = comm.size
        self.cache = {}

    def run(self):
        menu = dict((cmd, getattr(self, cmd)) for cmd in ("scatterGather", "clearCache", "exit",))
        print "Slave %d waiting for command" % self.rank
        command = self.comm.broadcast(None, root=self.root)
        while not menu[command]():
            print "Slave %d waiting for command" % self.rank
            command = self.comm.broadcast(None, root=self.root)
            print "Slave %d commanded: %s" % (self.rank, command)
        print "Slave %d complete" % self.rank

    def scatterGather(self):
        print "Slave %d waiting for func" % self.rank
        tags, func, args, passPool = self.comm.broadcast(None, root=self.root)
        print "Slave %d waiting for job" % self.rank
        job = self.comm.scatter(root=self.root)

        def runJob(job):
            index, data = job
            print "Slave %d running job: %s %s" % (self.rank, data, args)
            if passPool:
                result = func(self, data, *args)
            else:
                result = func(data, *args)
            self.comm.send((index, result), self.root, tag=tags.result)

        while not isinstance(job, NoOp):
            runJob(job)
            print "Slave %d waiting for job" % self.rank
            job = self.comm.recv(tag=tags.work, source=self.root)

        print "Slave %d done" % self.rank

    def clearCache(self):
        self.cache = {}

    def exit(self):
        sys.stderr.write("Slave %d exiting" % self.rank)
        return True

def Pool(root=0, slavesExitWhenDone=True):
    comm = Comm()
    if comm.rank == root:
        return PoolMaster(comm)
    pool = PoolSlave(comm, root=root)
    pool.run()
    if slavesExitWhenDone:
        pool = None
        exit()
    return pool


if __name__ == "__main__":
    pool = Pool()

    import math
    data = map(float, range(10))
    print "Calculating [sqrt(x) for x in %s]" % data
    result = pool.scatterGather(math.sqrt, False, data)
    print result
    del pool
#    pool.exit()
#    print "Now you can play yourself..."
#    import code
#    code.interact(local=locals())
