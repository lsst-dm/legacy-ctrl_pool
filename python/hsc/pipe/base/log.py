import lsst.pex.logging

__all__ = ["PicklableLog", "getDefaultLog"]

OriginalLog = lsst.pex.logging.Log
defaultLog = lsst.pex.logging.getDefaultLog()

class PicklableLog(object):
    def __init__(self, log=None, name=None, destination=None):
        if log is not None:
            if isinstance(log, PicklableLog):
                if destination is None:
                    destination = log._destination
                log = log._log
        else:
            log = defaultLog
        if name is not None:
            log = OriginalLog(log, name)
        self._log = log
        self._name = name
        self._destination = destination
        if destination is not None:
            self._log.addDestination(destination)
    def __getattr__(self, name):
        return getattr(self._log, name)
    def __reduce__(self):
        return self.__class__, (None, self._name, self._destination)

def getDefaultLog():
    return PicklableLog()

defaultLog.warn("Monkey-patching lsst.pex.logging with PicklableLog")
lsst.pex.logging.getDefaultLog = getDefaultLog
lsst.pex.logging.Log = PicklableLog
