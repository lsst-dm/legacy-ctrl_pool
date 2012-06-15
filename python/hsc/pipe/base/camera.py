#!/usr/bin/env python

import os
import errno
import lsst.daf.persistence as dafPersist

def getButler(instrument, rerun=None, **kwargs):
    """Return a butler for the appropriate instrument"""
    if rerun is None:
        rerun = os.environ["LOGNAME"]

    envar = "SUPRIME_DATA_DIR"

    if kwargs.get('root', None):
        root = kwargs['root']
    else:
        if not os.environ.has_key(envar):
            raise RuntimeError("You must define $%s ; did you setup suprime_data?" % envar)
        root = os.path.join(os.environ[envar], "SUPA")
        kwargs['root'] = root

    if not kwargs.get('outputRoot', None):
        outPath = os.path.join(root, "rerun", rerun)
        kwargs['outputRoot'] = outPath
        if not os.path.exists(outPath):
            # Subject to race condition
            try:
                os.makedirs(outPath) # should be in butler
            except OSError, e:
                if not e.errno == errno.EEXIST:
                    raise

    if instrument.lower() in ["hsc"]:
        import lsst.obs.hscSim as obsHsc
        mapper = obsHsc.HscSimMapper(**kwargs)
    elif instrument.lower() in ["suprimecam", "suprime-cam", "sc"]:
        import lsst.obs.suprimecam as obsSc
        mapper = obsSc.SuprimecamMapper(**kwargs)
    else:
        raise RuntimeError("Unrecognised instrument: %s" % instrument)



    return dafPersist.ButlerFactory(mapper=mapper).create()


def getNumCcds(instrument):
    """Return the number of CCDs in an instrument"""
    # XXX This could be done by inspecting the number of Ccds in butler.mapper.camera
    if instrument.lower() in ["hsc", "hscsim"]:
        return 104
    if instrument.lower() in ["suprimecam", "suprime-cam", "sc"]:
        return 10
    raise RuntimeError("Unrecognised instrument: %s" % instrument)

