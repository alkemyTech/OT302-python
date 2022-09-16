from pathlib import Path
import logging
from typing import Optional

"""
This function should be called when you want to configure logging.
Called: 'configure_logger( path, university_name)'.
You must instantiate the log record with 'logger = logging.getLogger()' to be used.
logger.info('message')
logger.warning('message')
logger.error('message')

"""


def configure_logger(
    log_path: Optional[str] = None, university: Optional[str] = "comahue"
) -> None:
    """The function sets the log file to an INFO level

    Args:
        log_path (Optional[str], optional): Path where the log file is created and written. Defaults to None.
        university (Optional[str], optional): The two possible values are 'comahue' and 'salvador'. Defaults to 'comahue'.
    """

    if university == "comahue":
        filename = "comahue_etl.log"
    else:
        filename = "salvador_etl.log"

    log_path_file = Path(log_path / filename) if log_path is not None else filename

    format_string = f"%(asctime)s - %(filename)s - %(message)s"

    logging.basicConfig(
        filname=log_path_file,
        level=logging.INFO,
        format=format_string,
        datefmt="%Y-%m-%d",
        filemode="w",
    )
