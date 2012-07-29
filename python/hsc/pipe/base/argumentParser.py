import argparse, os
from lsst.pipe.base import ArgumentParser

class SubaruArgumentParser(ArgumentParser):
    def __init__(self, *args, **kwargs):
        ArgumentParser.__init__(self, *args, **kwargs)
        self.add_argument('--rerun', type=str, default=None, help='Desired rerun (overrides --output)',
                          action="store", dest="rerun")

    def _fixPaths(self, namespace):
        if namespace.rerun and namespace.output:
            argparse.ArgumentTypeError("Please specify --output or --rerun, but not both")
        ArgumentParser._fixPaths(self, namespace)
        if namespace.rerun:
            root = os.environ.get("SUPRIME_DATA_DIR")
            if not root:
                argparse.ArgumentTypeError("Cannot use --rerun without setting up suprime_data")
            namespace.output = os.path.join(root, "rerun", namespace.rerun)
