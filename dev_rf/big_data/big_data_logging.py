import logging
from logging import config
import os


def set_logging_config():
    """This function is going to setup the ogger using the logger.cfg file.
    The logger has 2 functions:
    1 - Display the logging messages in the console
    2 - Save the files to a log file every week (Every Sunday)"""

    # Get current working directory
    cwd = os.getcwd()

    # Create logs folder if it does not exist
    try:
        if not os.path.exists(cwd + "/logs"):
            os.makedirs(cwd + "/logs")
    except:
        print("Folder cannot be created")

    # Load the logger.cfg file
    # cwd + "big_data/logger.cfg"
    config.fileConfig("logger.cfg")

    # Create logger with the configuration
    logger = logging.getLogger("root")

    return logger
