from pathlib import Path
import sys
import logging
from typing import Optional

"""
This function should be called when you want to configure logging.
Called: 'configure_logger(university_name)'.
"""

PATH_DATA = "data"


def configure_logger( university: Optional[str] = "comahue" ) -> object:
    """The function sets the log file to an INFO level for file logger and ERROR por console logger

    Args:
        log_path (Optional[str], optional): Path where the log file is created and written. Defaults to None.
        university (Optional[str], optional): The two possible values are 'comahue' and 'salvador'. Defaults to 'comahue'.
    """

    if university == "comahue":
        filename = "comahue_etl.log"
    else:
        filename = "salvador_etl.log"

    root = Path.cwd()
    log_path_file = Path(root / PATH_DATA / filename)

    # create logger
    logger = logging.getLogger(university)
    logger.setLevel(logging.INFO)

    # create file handler which logs even debug messages
    f_handler = logging.FileHandler(log_path_file, mode='w')
    f_handler.setLevel(logging.INFO)
    # create console handler with a higher log level
    c_handler = logging.StreamHandler(stream=sys.stdout)
    c_handler.setLevel(logging.ERROR)

    # create formatter and add it to the handlers
    format_string = f"%(asctime)s - %(filename)s - %(message)s"
    formatter = logging.Formatter(format_string, datefmt="%Y-%m-%d") 
    f_handler.setFormatter(formatter)
    c_handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(f_handler)
    logger.addHandler(c_handler)

    return logger
