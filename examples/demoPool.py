#!/usr/bin/env python

# To run:
#
#    mpiexec -n <number of nodes> /path/to/demoPool.py"
#

import math
def test1(cache, data, *args):
    result = math.sqrt(data)
    cache.result = result
    cache.args = args
    return result

def test2(cache, data, *args):
    result = math.sqrt(data)
    print "%d: %f vs %f ; %s vs %s" % (cache.comm.rank, cache.result, result, cache.args, args)
    return None

from hsc.pipe.base.pool import Pool, Debugger

Debugger().enabled = True
pool = Pool()
dataList = map(float, range(10))
args = ["foo", "bar"]

print "Calculating [sqrt(x) for x in %s]" % dataList
print pool.scatterGather(test1, True, dataList, *args)
print pool.scatterToPrevious(test2, dataList, *args)

# This is important to prevent a segmentation fault
del pool
