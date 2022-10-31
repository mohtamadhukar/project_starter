""" Adding loggers """

import logging.config
import warnings

from project_starter_lib.config.logging_config import LOGGING_CONFIG

""" Filter warnings in command line """
warnings.filterwarnings("ignore")

""" Initialize logging """
logging.config.dictConfig(LOGGING_CONFIG)