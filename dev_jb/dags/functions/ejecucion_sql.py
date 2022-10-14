# Librerias #
from mimetypes import guess_all_extensions
import pandas as pd
import re
from pathlib import Path
from sqlalchemy import create_engine
from airflow.models import Variable
from functions.logger_conf import logger

"""
Funcion: ejecuta las setencias sql del archivo seleccionado y guarda los datos en un archivo
.csv en la ubicacion especificada con el nombre de la universidad que figure en la query.
Args:
    sql_path (str, obligatorio): path donde esta almacenado el archivo .sql.
    sql_nombre (str, obligatorio): nombre del archivo .sql.
    guardado_path (str. obligatorio): path donde se guardaran los archivos .csv.
    cantidad_queries (int, opcional): cantidad de queries que tenga el archivo .sql.
Uso:
    sql_a_csv(path_a,
              'nombre.sql',
              path_b,
              2
    )
Variables de entorno: todas las varaibles deben estar almacenadas como varaibles en ariflow.
La conexion se hara a una base de datos postgresql.
                    pg_usuario = usuario
                    pg_contraseña = contraseña
                    pg_host = host
                    pg_puerto = puerto
                    pg_db = base de datos
"""

# Funcion principal #
def sql_a_csv(sql_path : str,
              sql_nombre : str,
              guardado_path : str,
              cantidad_queries = 1
):
    # Inicilizacion del logger
    logger_flores = logger(Path('/c/Users/Jeremy/airflow/logs'), 'universidad_de_flores.log')
    logger_villa_maria = logger(Path('/c/Users/Jeremy/airflow/logs'), 'universidad_nacional_de_villa_maría.log')
    
    # Log inicio del proceso
    logger_flores.info('Extracion de datos iniciada')
    logger_villa_maria.info('Extracion de datos iniciada') 
    
    # Conexion a la base de datos #
    # Configura el enalce de conexion a la base de datos especificada
    engine = get_engine(
             Variable.get('pg_usuario'),
             Variable.get('pg_contraseña'),
             Variable.get('pg_host'),
             Variable.get('pg_puerto'),
             Variable.get('pg_db')
    )
    
    # Log de inicializacion de la base de datos
    logger_flores.info('Conexion a la base de datos postgresql realizada con exito')
    logger_villa_maria.info('Conexion a la base de datos postgresql realizada con exito')
        
    # Lectura de queries #
    # Lee y guarda en una lista por separado cada query del archivo especificado
    with open(Path(sql_path, sql_nombre), encoding = 'utf8') as archivo:
        queries = archivo.read().split(';', cantidad_queries - 1)
    
    # Extraccion y guardado de datos #
    # Ejecuta la query correspondiente y la la guarda en un archiv .csv con el nombre de
    # correspondiente a la universidad a la que haga referencia la query. Este proceso se
    # ejecutara para cada query
    for query in queries:
        extraccion_gurdado(query,
                           guardado_path,
                           engine, 
                           logger_flores,
                           logger_villa_maria
        )

# Funciones esclavas #
"""
Funcion: generara el enlace de conexion con la base de datos
Args:
    usuario (str, obligatorio): usuario de acceso.
    contraseña (str, obligatorio): contraseña de acceso.
    host (str, obligatorio): host de la base de datos sin http://.
    puerto (str, obligatorio): puerto de la base de datos.
    db (str, obligatorio): nombre de la base de datos.
Uso:
    engine = get_engine(usuario,
               contraseña,
               host,
               puerto,
               db
    )
"""
def get_engine(usuario : str,
               contraseña : str,
               host : str,
               puerto : str,
               db : str
):
    # Genera el enlace a la base de datos con los parametros proporcionados y los devuelve
    return create_engine(f"postgresql://{usuario}:{contraseña}@{host}:{puerto}/{db}", echo=False)

"""
Funcion: ejecuta la query especificada en el engine dado y guarda los archivos con el nombre de la universidad
que figura en la query en la ruta deseada.
Args:
    query (str, obligatorio): query a ejecutar.
    guardado_path (str, obligatorio): path de guardado para los archivos .csv.
    enigne (objeto, obligatorio): engine de conexion a la base de datos.
Uso:
    extraccion_gurdado(query,
                       path_b,
                       engine
    )
"""
def extraccion_gurdado(query : str,
                       guardado_path : str,
                       engine : object,
                       logger_f : object,
                       logger_vm : object
):
    # Ejecuta la query proporcionada sobre el cursor de la base de datos proporcionada
    tabla = pd.read_sql_query(query, engine)
    
    # Guarda los datos en un archivo .csv en la ruta especificada con el nombre
    # correspondiente a la universidad a la que haga referencia la query
    tabla.to_csv(Path(guardado_path, nombre_csv(query)), sep = ',', index = False, encoding = 'utf8')
    
    # Log confirmacion guardado de datos
    if nombre_csv(query) == Path('universidad_de_flores.csv'):
        logger_f.info(f'Guardado de datos corectamente en {guardado_path} en formato .csv, separado por ","')
    else:
        logger_vm.info(f'Guardado de datos corectamente en {guardado_path} en formato .csv, separado por ","')
    
"""
Funcion: extraer el nombre de la universidad
Args:
    query (str, obligatorio): query de donde se extraera el nombre.
Uso:
    nombre = nombre_csv(query)
"""
def nombre_csv(query : str
):
    # Devuele el nombre de la universidad que figura en la query
    r = re.compile(r"'(UNIVERSIDAD.*\D)' ")
    return Path(r.search(query.replace('\n', ' ')).groups(0)[0].replace(' ', '_').lower()).with_suffix('.csv')