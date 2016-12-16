from future import standard_library
standard_library.install_aliases()
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
    machine = os.uname()[1].split(".")[0]
    packageDir = getPackageDir("ctrl_pool")
    #   Set the environment variable which names the output file
    os.environ['JOBNAME'] = job
    lsstLog.configure(os.path.join(packageDir, "config/log4cxx.properties"))
    lsstLog.MDC("PID", os.getpid())
