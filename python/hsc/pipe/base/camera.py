#!/usr/bin/env python

import os
import lsst.daf.persistence as dafPersist

def getButler(instrument, rerun=None, **kwargs):
    """Return a butler for the appropriate instrument"""
    if rerun is None:
        rerun = os.getlogin()

    envar = "SUPRIME_DATA_DIR"
    if not os.environ.has_key(envar):
        raise RuntimeError("You must define $%s ; did you setup suprime_data?" % envar)
    inPath = os.path.join(os.environ[envar], "SUPA")
    outPath = os.path.join(os.environ[envar], "SUPA", "rerun", rerun)
    if not os.path.exists(outPath):
        os.makedirs(outPath) # should be in butler
    if not 'root' in kwargs: kwargs['root'] = inPath
    if not 'outputRoot' in kwargs: kwargs['outputRoot'] = outPath

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

