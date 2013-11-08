import lsst.pex.logging

__all__ = ["PicklableLog", "getDefaultLog"]

OriginalLog = lsst.pex.logging.Log
defaultLog = lsst.pex.logging.getDefaultLog()

class PicklableLog(object):
    """Log that can be pickled

    Wraps the LSST default log object and reinstantiates
    it on the other side, re-adding all the destinations.
    Assumes:
    1. The only log in use is the default log.
    2. The only actions on the log beside logging that change
       the log state are adding output destinations.
    """
    def __init__(self, log=None, name=None, destination=[]):
        """Constructor

        @param log: Log to wrap; None for LSST default log
        @param name: Name to give log, or None
        @param destination: List of log files to write
        """
        if log is not None:
            if isinstance(log, PicklableLog):
                destination.extend(log._destination)
                log = log._log
        else:
            log = defaultLog
        if name is not None:
            log = OriginalLog(log, name)
        self._log = log
        self._name = name
        self._destination = destination
        for dest in destination:
            self._log.addDestination(dest)
    def __getattr__(self, name):
        return getattr(self._log, name)
    def __reduce__(self):
        return self.__class__, (None, self._name, self._destination)
    def addDestination(self, dest):
        self._destination.append(dest)
        self._log.addDestination(dest)

def getDefaultLog():
    return PicklableLog()

defaultLog.warn("Monkey-patching lsst.pex.logging with PicklableLog")
lsst.pex.logging.getDefaultLog = getDefaultLog
lsst.pex.logging.Log = PicklableLog
