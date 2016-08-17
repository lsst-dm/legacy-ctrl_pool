import math
import collections
from lsst.ctrl.pool.parallel import BatchPoolTask
from lsst.ctrl.pool.pool import Pool
from lsst.pipe.base import ArgumentParser
from lsst.pex.config import Config

__all__ = ["DemoTask", ]


class DemoTask(BatchPoolTask):
    """Task for demonstrating the BatchPoolTask functionality"""
    ConfigClass = Config
    _DefaultName = "demo"

    @classmethod
    def _makeArgumentParser(cls, *args, **kwargs):
        kwargs.pop('doBatch', False)  # Unused
        parser = ArgumentParser(name="demo", *args, **kwargs)
        parser.add_id_argument("--id", datasetType="raw", level="visit",
                               help="data ID, e.g. --id visit=12345")
        return parser

    @classmethod
    def batchWallTime(cls, time, parsedCmd, numCores):
        """Return walltime request for batch job

        Subclasses should override if the walltime should be calculated
        differently (e.g., addition of some serial time).

        @param time: Requested time per iteration
        @param parsedCmd: Results of argument parsing
        @param numCores: Number of cores
        """
        numTargets = [sum(1 for ccdRef in visitRef.subItems("ccd") if ccdRef.datasetExists("raw")) for
                      visitRef in parsedCmd.id.refList]
        return time*sum(math.ceil(tt/numCores) for tt in numTargets)

    def run(self, visitRef):
        """Main entry-point

        Only the master node runs this method.  It will dispatch jobs to the
        slave nodes.
        """
        pool = Pool("test")

        # Less overhead to transfer the butler once rather than in each dataRef
        dataIdList = dict([(ccdRef.get("ccdExposureId"), ccdRef.dataId)
                           for ccdRef in visitRef.subItems("ccd") if ccdRef.datasetExists("raw")])
        dataIdList = collections.OrderedDict(sorted(dataIdList.items()))

        with self.logOperation("master"):
            pixels = pool.map(self.read, dataIdList.values(), butler=visitRef.getButler())
        total = sum(pp for pp in pixels if pp is not None)
        self.log.info("Total number of pixels read: %d" % (total,))

    def read(self, cache, dataId, butler=None):
        """Read image and return number of pixels

        Only the slave nodes run this method.
        """
        assert butler is not None
        with self.logOperation("read %s" % (dataId,)):
            raw = butler.get("raw", dataId, immediate=True)
            dims = raw.getDimensions()
            num = dims.getX()*dims.getY()
            self.log.info("Read %d pixels for %s" % (num, dataId,))
            return num

    def _getConfigName(self):
        return None

    def _getMetadataName(self):
        return None

    def _getEupsVersionsName(self):
        return None
