from future import standard_library
standard_library.install_aliases()
import os
import copyreg

import lsst.log as lsstLog
import lsst.pex.logging as pexLog
from lsst.utils import getPackageDir



def pickleLog(log):
    """Pickle a log

    Drop the log on the floor; we'll get a new default.
    Assumes that we're always just using the default log,
    and not modifying it.
    """
    return pexLog.getDefaultLog, tuple()


def pickleLsstLog(log):
    """Pickle a lsst.log.Log"""
    return lsstLog.Log, tuple()

copyreg.pickle(pexLog.Log, pickleLog)
copyreg.pickle(pexLog.ScreenLog, pickleLog)
copyreg.pickle(lsstLog.Log, pickleLsstLog)


def jobLog(job):
    """Add a job-specific log destination"""
    if job is None or job == "None":
        return
    machine = os.uname()[1].split(".")[0]
    pexLog.getDefaultLog().addDestination(job + ".%s.%d" % (machine, os.getpid()))
    packageDir = getPackageDir("ctrl_pool")
    lsstLog.configure(os.path.join(packageDir, "config/log4cxx.properties"))
    lsstLog.MDC("PID", os.getpid())
