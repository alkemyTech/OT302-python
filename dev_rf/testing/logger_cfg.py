# Modulos #
from pathlib import Path
import logging
from logging import config

# Path donde se encuentra el archivo de configuracion .cfg
path_file = 'D:\Alkemy\OT302-python\dev_jb\big_data\logger'

# Nombre del archivo de configuracion .cfg
filename_cfg = "logger_config.cfg"


def logger_comfig():
    # Genera el path de configuracion
    log_config_path = Path(path_file, filename_cfg)

    # Configura el logger
    config.fileConfig(log_config_path)
    logger = logging.getLogger('log')

    # Devuelve el logger
    return logger