# Logging with config files

# Modules
import logging
from logging.config import fileConfig
from pathlib import Path

# Functions
def bigdata_logger(
    config_path = 'logger_config.cfg'
    ):
    """
    Creates logger and returns from cfg file pass as a parameter.
    If file not exists raise FileNotFoundError.
    Args:
        config_path (str, optional): Config file name. Defaults to 'logger_config.cfg'.
    Returns:
        logger: logger object created from config file settings
    """
    # Abs path from file using pathlib.Path
    abs_path = Path.cwd() / config_path
    # Check if config file exists
    if not abs_path.exists():
        raise FileNotFoundError(f'File {config_path} not found.')
    # Logger form settings and __name__ module
    fileConfig(fname = abs_path)
    logger = logging.getLogger(__name__)
    return logger