# Librerias #
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from functions.logger_conf import logger
from pathlib import Path

"""
Funcion: carga el archivo deseado a una bucket de S3 previamente configurado en airflow
Args:
    archivo_path (str, obligatorio): path donde se encuentra almacenado el archivo
    archivo_nom (str, obligatorio): nombre del archivo a cargar
    aws_conn (json, obligatorio): credenciales de Amazon S3
    key (str, opcional): puntero que indica donde se guardara el archivo en S3
    bucket (str, opcional): nombre del bucket de S3 donde se almacenara el archivo
Uso:
    carga_s3('parh_a', 'nombre', aws_conn)
"""

# Funcion principal #
def carga_s3(archivo_path : str,
             archivo_nom : str,
             aws_conn : str,
             key = 'DA-302',
             bucket = 'cohorte-septiembre-5efe33c6',
):
    # Inicializacion del logger
    logger_a = logger(Path('/c/Users/Jeremy/airflow/logs'), str(Path(archivo_nom.split('.')[0]).with_suffix('.log')))
    
    # Log inicio del proceso
    logger_a.info(f'Carga del archiv {archivo_nom} a S3 iniciada')
    
    # Conexion a S3
    try:
        hook = S3Hook(aws_conn_id = aws_conn)
    except Exception as ex:
        logger.error(f'Error al intentar la conexion a S3: {ex}')
        raise ex
    
    # Carga de archivo
    hook.load_file(filename = Path(archivo_path, archivo_nom),
                   key = key + '/' + archivo_nom,
                   bucket_name = bucket,
                   replace = True)
    
    # Log de aviso de carga del archivo
    logger_a.info(f'Carga del archivo {archivo_nom} a S3 finalizada correctamente')

