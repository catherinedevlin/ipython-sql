import argparse
import sys
from sql import exceptions


class CmdParser(argparse.ArgumentParser):
    """
    Subclassing ArgumentParser as it throws a SystemExit
    error when it encounters argument validation errors.


    Now we raise a UsageError in case of argument validation
    issues.
    """

    def exit(self, status=0, message=None):
        if message:
            self._print_message(message, sys.stderr)

    def error(self, message):
        raise exceptions.UsageError(message)
