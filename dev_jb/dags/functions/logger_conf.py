# Librerias #
import logging
from pathlib import Path

"""
    Funcion: crea y devuelve un logger con los parametros especificados.
    Nivel del logger: INFO
    Args: 
        logger_path (str, obligatorio): path donde se creara la carpeta logs y se almacenara el archivo de .log.
        logger_nombre_archivo (str, obligatorio): nombre del archivo .log.
        logger_fmt (str, opcional): formato que adoptaran los mensajes. Por defecto sera '%(asctime)s - %(name)s - %(message)s' 
        logger_fecha_fmt (DateString, opcional): formato de fecha. Por defecto sera '%Y-%m-%d'
    Devuelve:
        (logging.logger): objeto tipo logger
    Uso:
        x_logger = logger(
                   logger_path = 'path',
                   logger_nombre_archivo = 'logger_x',
                   logger_fmt = '%(asctime)s - %(name)s - %(message)s',
                   logger_fecha_fmt = '%Y-%m-%d'
                    )
                    
        x_logger.info('mensaje')
"""

# Funcion #
def logger(
    logger_path : str,
    logger_nombre_archivo : str,
    logger_fmt = '%(asctime)s - %(name)s - %(message)s',
    logger_fecha_fmt = '%Y-%m-%d'
):
    
    # Corroboracion de directorios #
    if not Path(logger_path, 'ETL_logs').exists():
        Path(logger_path, 'ETL_logs').mkdir()
    
    # Creacion del logger #
    # Crea el logger
    aux_logger = logging.getLogger(logger_nombre_archivo)
    
    # Fija el nivel del logger
    aux_logger.setLevel(logging.INFO)
    
    # Crea el Handler
    handler = logging.FileHandler(Path(logger_path, 'ETL_logs', logger_nombre_archivo).with_suffix('.log'))
    
    # Fija el formato del logger
    handler.setFormatter(logging.Formatter(fmt = logger_fmt, datefmt = logger_fecha_fmt))
    
    # Se agrega el handler al logger
    aux_logger.addHandler(handler)
    
    # Devuelve el logger configurado
    return aux_logger