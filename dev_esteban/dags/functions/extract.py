import sys, os, pandas as pd
from typing import List
from sqlalchemy import create_engine
from decouple import RepositoryEnv
from functions.logging_setup_c import CustomLogger


# custom_config = RepositoryEnv('ot302-python/dev_esteban/dags/.env')
custom_config = RepositoryEnv('/Users/sergioboada/.airflow/ot302-python/.env')

logger = CustomLogger('jujuy','palermo')
jujuy_logger = logger.get_logger('jujuy')
palermo_logger = logger.get_logger('palermo')
    
def extract(path_csv_dir: str, sql_scripts: List, db_connection: str) -> None:
    #Crear conexion a base de datos
    db = _setup_connection()

    try:
        # Leer comando sql pora extraccion de datos
        with open(sql_scripts[0], mode='r') as file:
            command_jujuy = file.read()
            file.close()
        # Obtener datos y convertirlos a csv
        jujuy_df = pd.read_sql(command_jujuy, db)
        jujuy_df.to_csv(
            path_or_buf=path_csv_dir+'jujuy.csv',
            index=False,
            na_rep=pd.NA
        )
        jujuy_logger.info('jujuy.csv loaded successfully')
    except Exception as e:
        jujuy_logger.debug(f"Cannot load {path_csv_dir}jujuy.csv ({e.args})")
    
    try:
        # Leer comando sql pora extraccion de datos
        with open(sql_scripts[1], mode='r') as file:
            command_palermo = file.read()
            file.close()
        # Obtener datos y convertirlos a csv
        palermo_df = pd.read_sql(command_palermo, db)
        palermo_df.to_csv(
            path_or_buf=path_csv_dir+'palermo.csv',
            index=False,
            na_rep=pd.NA
        )
        jujuy_logger.info('palermo.csv loaded successfully')
    except Exception as e:
        palermo_logger.debug(f"Cannot load {path_csv_dir}palermo.csv ({e.args})")

def _setup_connection():
    engine = custom_config.data['ENGINE']
    user = custom_config.data['USR']
    password = custom_config.data['PASSWORD']
    port = custom_config.data['PORT']
    host = custom_config.data['HOST']
    database = custom_config.data['DATABASE']
    link =""\
        f"{engine}+psycopg2://{user}:"\
        +f"{password}@{host}:"\
        +f"{port}/{database}"
    try:
        db = create_engine(link) 
        return db
    except Exception as e:
        jujuy_logger.critical(f"Error to connect to database {database}")
        palermo_logger.critical(f"Error to connect to database {database}")
        return None







    


    
    