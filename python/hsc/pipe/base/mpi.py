import os
import sys
import signal
from functools import wraps

__all__ = ["thisNode", "abortOnError"]

os.umask(002)
def sigalrm_handler(signum, frame):
    sys.stderr.write('Signal handler called with signal %s\n' % (signum))
signal.signal(signal.SIGALRM, sigalrm_handler)

def thisNode():
    """Return a short string to identify the node: <name>:<pid>"""
    return "%s:%d" % (os.uname()[1], os.getpid())

def abortOnError(func):
    """Decorator to throw an MPI abort on an unhandled exception"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception, e:
            sys.stderr.write("%s on %s in %s: %s\n" % (type(e).__name__, thisNode(), func.__name__, e))
            import traceback
            traceback.print_exc(file=sys.stderr)
            import mpi4py.MPI as mpi
            mpi.COMM_WORLD.Abort(1)
    return wrapper
