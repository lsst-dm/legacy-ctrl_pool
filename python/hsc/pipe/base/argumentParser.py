import argparse, os
from lsst.pipe.base import ArgumentParser

class SubaruArgumentParser(ArgumentParser):
    def __init__(self, *args, **kwargs):
        super(SubaruArgumentParser, self).__init__(*args, **kwargs)
        self.add_argument('--rerun', type=str, default=None, help='Desired rerun (overrides --output)',
                          action="store", dest="rerun")

    def _fixPaths(self, namespace):
        if namespace.rerun and namespace.output:
            argparse.ArgumentTypeError("Please specify --output or --rerun, but not both")
        super(SubaruArgumentParser, self)._fixPaths(namespace)
        if namespace.rerun:
            root = os.environ.get("SUPRIME_DATA_DIR")
            if not root:
                argparse.ArgumentTypeError("Please specify --output or --rerun, but not both")
            namespace.output = os.path.join(root, "rerun", namespace.rerun)
