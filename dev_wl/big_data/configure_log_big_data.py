"""
This function configure the logs,
shows them in the console and records two files that are rotated every 7 days,
it is set so that they change on Monday of each week.

"""
from pathlib import Path
import logging
from logging import config

# Path where is file.cfg
PATH_DATA = "dev_wl/big_data/"


def configure_log_big_data():
    """  This function configures the logger through the configure_logger.cfg file.
         When reading the configuration file, the StreamHandler and handlers.TimedRotatingFileHandler
         classes of loggin will be instantiated to print the logger in the console
         and in two files that will rotate every 7 days, it is set on Mondays.
    """
    # I take the directory from where the script is running
    root = Path.cwd()

    # I set the name of the file containing the logger configuration in configparser format.
    filename_cfg = "configure_logger.cfg"

    # the path to the file is armed
    log_path_file = Path(root / PATH_DATA / filename_cfg)

    # logging is configured
    config.fileConfig(log_path_file)
    logger = logging.getLogger('root')

    return logger