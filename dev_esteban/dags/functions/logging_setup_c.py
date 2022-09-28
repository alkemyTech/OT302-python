import logging as lg
from unicodedata import name

class custom_logger():

    
    def log_file(name_logger: str):
        lg.basicConfig(
        # lg.Logger(       
            filename="uni_c.log",
            filemode='w',
            level=lg.INFO,
            format="%(asctime)s-%(name_logger)s-%(message)s",
            datefmt='%Y-%m-%d',
        )

    def add_log(message: str):
        lg.log(
            level=lg.DEBUG,
            msg=f"{message}"
        )
        

        # logging.Logger()

