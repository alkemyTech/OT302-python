#import tools
from airflow import DAG
from datetime import timedelta, datetime
import os

from functions.extract import extract
from functions.transform import process_data
from functions.load_boto3 import load_jujuy, load_palermo

# Import operators
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

# path = os.getcwd()+"/dev_esteban/dags"
path = '/Users/sergioboada/.airflow/ot302-python/dev_esteban/dags'
# Creacion default de args
default_args={
    'owner':'dev esteban boada',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

# Definir DAG
with DAG(
    dag_id='uni_c_v01',
    default_args=default_args,
    description='''
    Ejecutar tareas de universidades del grupo C.
        * Universidad de Palermo
        * Universidad Nacional de Jujuy
    ''',
    start_date=datetime(2022, 9, 27),
    schedule_interval='0 * * * *',
    catchup=False

) as dag:
    # Obtener info de DB y escribir archivo original
    query=PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        op_kwargs={
            'path_csv_dir': f"{path}/files/",
            'sql_scripts': [f"{path}/scripts/uni_jujuy.sql",f"{path}/scripts/uni_palermo.sql"],
            'db_connection': 'alkemy_psql'
        }

    )
    # Depurar archivo original y crear uno nuevo
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=process_data,
        op_kwargs={
            'path_csv_dir': f"{path}/files/"
        }
    )

    load_jujuy_data = PythonOperator(
        task_id='load_jujuy_data_v01',
        python_callable=load_jujuy,
        op_kwargs={
            'path_csv_dir': f"{path}/files/"
        }
    )

    load_palermo_data = PythonOperator(
        task_id='load_palermo_data_v01',
        python_callable=load_palermo,
        op_kwargs={
            'path_csv_dir': f"{path}/files/"
        }
    )
  
    query >> transform >> [load_jujuy_data, load_palermo_data]