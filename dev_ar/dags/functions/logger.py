# Modules
import logging
#import os

#print(os.path.relpath(__file__))

# Functions
def logger(
    logger_name = 'test',
    logger_format = '%(asctime)s - %(name)s - %(message)s',
    logger_datefmt = '%Y-%m-%d',
    logger_file_path = './logs/logs.txt'
):
    """
    Create and return logger using loggin module with parameters passed
    Set to INFO level
    Use Example: any_logger = logger(logger_name = 'any logger') // any_logger.info('Message')
    Args:
        logger_name (str, optional): Logger name. Defaults to 'test'.
        logger_format (str, optional): Log format. Defaults to '%(asctime)s - %(name)s - %(message)s'.
        logger_datefmt (str, optional): Format time in datetime format. Defaults to '%Y-%m-%d'.
        logger_file_path (str, optional): Log file name and relative path. Defaults to './logs/logs.txt'.

    Returns:
        (logging.logger) : logging logger object
    """
    # logging.basicConfig(
    #     filename = logger_file_path,
    #     format = logger_format,
    #     datefmt = logger_datefmt,
    #     level = logging.INFO,
    # )

    # Create Logger
    custom_logger = logging.getLogger(logger_name)
    # Set Level
    custom_logger.setLevel(logging.INFO)
    # Create File Handler
    # Check also StreamHandler() with sys.stdout
    file_handler = logging.FileHandler(logger_file_path)
    # Create Formatter and add It to Handler
    custom_formatter = logging.Formatter(
        fmt = logger_format,
        datefmt = logger_datefmt
        )
    file_handler.setFormatter(custom_formatter)
    # Add File Handler to Looger
    custom_logger.addHandler(file_handler)
    # Return Logger
    return custom_logger