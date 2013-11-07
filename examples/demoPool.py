#!/usr/bin/env python

# To run:
#
#    mpiexec -n <number of nodes> /path/to/demoPool.py"
#

import math
def test1(cache, data, *args):
    result = math.sqrt(data)
    print "Store: %s" % ("present" if hasattr(cache, "store") else "absent")
    cache.result = result
    cache.args = args
    return result

def test2(cache, data, *args):
    result = math.sqrt(data)
    print "%d: %f vs %f ; %s vs %s ; %s" % (cache.comm.rank, cache.result, result, cache.args,
                                            args, hasattr(cache, "store"))
    return None

from hsc.pipe.base.pool import startPool, Debugger

Debugger().enabled = True
pool = startPool()
dataList = map(float, range(10))
args = ["foo", "bar"]
pool.storeSet("store", 1)

print "Calculating [sqrt(x) for x in %s]" % dataList
print pool.scatterGather(test1, True, dataList, *args)
pool.clearCache()
pool.storeDel("store")
print pool.scatterGatherNoBalance(test1, True, dataList, *args)
pool.storeSet("store", 2)
pool.storeClear()
print pool.scatterToPrevious(test2, dataList, *args)

# This is important to prevent a segmentation fault
del pool
