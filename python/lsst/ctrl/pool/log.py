import logging
import os
import copyreg
import lsst.log as lsstLog
from lsst.utils import getPackageDir


def pickleLog(log):
    """Pickle a log

    Assumes that we're always just using the lsst.log default.
    """
    return lsstLog.Log, tuple()


copyreg.pickle(lsstLog.Log, pickleLog)


def jobLog(job):
    """Add a job-specific log destination"""
    if job is None or job == "None":
        return
    packageDir = getPackageDir("ctrl_pool")
    #   Set the environment variable which names the output file
    os.environ['JOBNAME'] = job
    lsstLog.configure(os.path.join(packageDir, "config/log4cxx.properties"))
    lsstLog.MDC("PID", os.getpid())

    # Forward python logging to lsst.log
    lgr = logging.getLogger()
    lsst_log_level = lsstLog.getDefaultLogger().getEffectiveLevel()
    lgr.setLevel(lsstLog.LevelTranslator.lsstLog2logging(lsst_log_level))
    lgr.addHandler(lsstLog.LogHandler())
