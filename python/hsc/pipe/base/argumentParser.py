import argparse, os, sys
from lsst.pipe.base import ArgumentParser


class OutputAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if namespace.rerun:
            raise argparse.ArgumentTypeError("Please specify --output or --rerun, but not both")

        namespace.outPath = values

class RerunAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        """We can't just parse the arguments and reset namespace.outPath as the Mapper's been
        instantiated before we get a chance"""

        if namespace.outPath:
            raise argparse.ArgumentTypeError("Please specify --output or --rerun, but not both")

        envar = "SUPRIME_DATA_DIR"
        if os.environ.has_key(envar):
            namespace.rerun = values
            namespace.outPath = os.path.join(os.environ[envar], "SUPA", "rerun", namespace.rerun)
            if not os.path.exists(namespace.outPath):
                os.makedirs(namespace.outPath) # should be in butler
        else:
            raise argparse.ArgumentTypeError("You must define $%s to use --rerun XXX" % envar)


class HscArgumentParser(ArgumentParser):
    def __init__(self, *args, **kwargs):
        if not 'conflict_handler' in kwargs:
            kwargs['conflict_handler'] = 'resolve'
        super(HscArgumentParser, self).__init__(*args, **kwargs)
        self.add_argument('--output', type=str, dest="outPath", default=None, help="output root directory",
                          action=OutputAction)
        self.add_argument('--rerun', type=str, default=None, help='Desired rerun (overrides --output)',
                          action=RerunAction)
