import logging
import os


class CustomLogger(logging.getLoggerClass()):
    """
    Creates a custom logger class

    Attributes
    ----------
        logger_name : str
            a name for the logger to be printed in the message (default is logger)
        file_name : str
            file name and format to save the file (default is dag_log.log)
        set_level : logging.attr
            set the logging level to log (default is INFO)
        file_path : str
            file path to the logs file (default is ./logs/)
        message_format : str
            format for the message that will be displayed in the log (default is "%(asctime)s - %(name)s - %(message)s")
        date_format : str
            date format to be used in the logs (default is "%Y/%m/%d")

    """

    def __init__(self, logger_name="logger", file_name="dag_log.log"):
        # Initialize the default values
        self.logger_name = logger_name
        self.level = logging.INFO
        self.file_name = file_name
        self.file_path = "./logs/"
        self.message_format = "%(asctime)s - %(name)s - %(message)s"
        self.date_format = "%Y/%m/%d"

        # Create custom logger
        super().__init__(self.logger_name)

        # Set the level
        self.setLevel(self.level)

        # Create folder if it does not exist
        try:
            if not os.path.exists(self.file_path):
                os.makedirs(self.file_path)
        except:
            print("Folder cannot be created")

        # Create fileHandler to save the log to a file
        self.file_handler = logging.FileHandler(self.file_path + f"{self.file_name}.log")
        self.file_handler.setLevel(self.level)

        # Format of the log messages
        self.formatter = logging.Formatter(self.message_format, datefmt=self.date_format)

        # Set format to the fileHandler
        self.file_handler.setFormatter(self.formatter)

        # Add FileHandler to the logger
        self.addHandler(self.file_handler)
