#!/usr/bin/env python

# To run:
#
#    mpiexec -n <number of nodes> /path/to/demoPool.py"
#

import math
def test1(cache, data, *args, **kwargs):
    result = math.sqrt(data)
    print "Store: %s" % ("present" if hasattr(cache, "p") else "absent")
    cache.result = result
    cache.args = args
    cache.kwargs = kwargs
    return result

def test2(cache, data, *args, **kwargs):
    result = math.sqrt(data)
    print "%d: %f vs %f ; %s vs %s ; %s vs %s ; %s" % (cache.comm.rank, cache.result, result, cache.args,
                                                       args, cache.kwargs, kwargs, hasattr(cache, "p"))
    return None

from hsc.pipe.base.pool import startPool, Pool, Debugger, Comm

# Here's how to enable debugging messages from the pool
Debugger().enabled = True

startPool()

dataList = map(float, range(10))

def context1(pool1):
    pool1.storeSet(p=1)
    print "Calculating [sqrt(x) for x in %s]" % dataList
    print "And checking for 'p' in our pool"
    print pool1.map(test1, dataList, "foo", foo="bar")

# Now let's say we're somewhere else and forgot to hold onto pool1
def context2(pool2):
    # New context: should have no 'p'
    fruit = ["tomato", "tomahtoe"]
    veges = {"potato": "potahtoe"}
    print pool2.mapNoBalance(test1, dataList, *fruit, **veges)
    print pool2.mapToPrevious(test2, dataList, *fruit, **veges)

pool1 = Pool(1)
context1(pool1)
pool2 = Pool(2)
context2(pool2)

Pool().exit() # This is important, to bring everything down nicely; or the wheels will just keep turning
# Can do stuff here, just not use any MPI because the slaves have exited.
# If you want the slaves, then pass "killSlaves=False" to startPool(); they'll emerge after startPool().
print "Done."

