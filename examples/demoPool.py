#!/usr/bin/env python

# To run:
#
#    mpiexec -n <number of nodes> /path/to/demoPool.py"
#
# I suggest running with n=1, 3, NUM nodes to cover all the operational modes.
#

from __future__ import print_function
from builtins import map
from builtins import range
NUM = 10  # Number of items in data list

import math


def test1(cache, data, *args, **kwargs):
    result = math.sqrt(data)
    print("Store: %s" % ("present" if hasattr(cache, "p") else "absent"))
    cache.result = result
    cache.args = args
    cache.kwargs = kwargs
    return result


def test2(cache, data, *args, **kwargs):
    result = math.sqrt(data)
    print("%d: %f vs %f ; %s vs %s ; %s vs %s ; %s" % (cache.comm.rank, cache.result, result, cache.args,
                                                       args, cache.kwargs, kwargs, hasattr(cache, "p")))
    return None

from lsst.ctrl.pool.pool import startPool, Pool, Debugger, Comm

# Here's how to enable debugging messages from the pool
Debugger().enabled = True

startPool()

dataList = list(map(float, range(NUM)))


def context1(pool1):
    pool1.storeSet(p=1)
    print("Calculating [sqrt(x) for x in %s]" % dataList)
    print("And checking for 'p' in our pool")
    print(pool1.map(test1, dataList, "foo", foo="bar"))

# Now let's say we're somewhere else and forgot to hold onto pool1


def context2(pool2):
    # New context: should have no 'p'
    fruit = ["tomato", "tomahtoe"]
    veges = {"potato": "potahtoe"}
    print(pool2.mapNoBalance(test1, dataList, *fruit, **veges))
    print(pool2.mapToPrevious(test2, dataList, *fruit, **veges))


def context3(pool3):
    # Check cache/store functionality
    pool3.storeSet(p=1, q=2)
    print(pool1.map(test1, dataList, "foo", foo="bar"))
    pool3.storeDel("p")
    pool3.storeList()
    pool1.cacheList()
    pool1.cacheClear()
    pool3.storeClear()
    pool3.storeList()

pool1 = Pool(1)
context1(pool1)
pool2 = Pool(2)
context2(pool2)
pool3 = Pool(3)
context3(pool3)

Pool().exit()  # This is important, to bring everything down nicely; or the wheels will just keep turning
# Can do stuff here, just not use any MPI because the slaves have exited.
# If you want the slaves, then pass "killSlaves=False" to startPool(); they'll emerge after startPool().
print("Done.")
