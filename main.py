#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRIPT_NAME
DESCRIPTION

Usage: SCRIPT_NAME.py [-hv]
       SCRIPT_NAME.py [--logLevel=<LOGLEVEL>][--environment=<ENV>]

Options:
  -h --help             Show this screen.
  -v --version          Show version.
  --logLevel=LOGLEVEL   The level of the logging output  [default: INFO]
  --environment=ENV     The execution environment
                        (development, staging or production)  [default: development]
"""

__appname__ = "APPLICATION_NAME"
__author__  = "Gerald Bäck (https://github.com/geraldbaeck/)"
__version__ = "0.0.1"
__license__ = "UNLICENSE"

DEFAULT_LOGLEVEL    = "INFO"
DEFAULT_ENVIRONMENT = "development"

# System libraries

# 3rd party libraries
from docopt import docopt
from logzero import logger

# Own libraries

def set_up_logging(loglevel=DEFAULT_LOGLEVEL, logtofile=True):
    try:
        # setup `logzero` module
        logzero.loglevel(getattr(logging, loglevel))

        # Setup rotating logfile with 3 rotations, each with a maximum filesize of 1MB:
        if logtofile:
            logzero.logfile("tmp/%s.log".format(__file__.replace(".py", "")), maxBytes=1e6, backupCount=3)

    except AttributeError:
        raise Exception(
            "Unsupported logLevel. Use [DEBUG, INFO, WARNING, ERROR, CRITICAL]")


def main():
    """ Main entry point of the app """
    with open('data/lumesakt-v2.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        print(row)


if __name__ == "__main__":
    """ This is executed when run from the command line """
    arguments = docopt(__doc__, version=__version__)
    ENVIRONMENT = arguments['--environment']
    set_up_logging(arguments['--logLevel'])
    logger.debug("%s started".format(__appname__))
    main()
