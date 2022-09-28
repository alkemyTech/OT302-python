#import tools
from airflow import DAG
from datetime import timedelta, datetime
import os

from functions.extract import *
from functions.transform import ProcessData

# Import operators
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash import BashOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

path = os.getcwd()+"/ot302-python/dev_esteban/dags"
# creating default config for dag
default_args={
    'owner':'dev esteban boada',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

# definning DAG
with DAG(
    dag_id='dag_uni_c_retries_v01',
    default_args=default_args,
    description='''
    Ejecutar tareas de universidades del grupo C.
        * Universidad de Palermo
        * Universidad Nacional de Jujuy
    ''',
    start_date=datetime(2022, 9, 27),
    schedule_interval='@daily',
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
    # # Depurar archivo original y crear uno nuevo
    # transform = PythonOperator(
    #     task_id='transform_data',
    #     python_callable=,
    #     op_kwargs={
    #         'path_csv_dir': f"{path}/files/",
    #         'sql_files':[f"{path}/scripts/uni_jujuy.sql",f"{path}/scripts/uni_palermo.sql"]
    #     }
    # )
  
# Define task.
    '''
    Operators a implementar:

    #Initi PostgreSQL Server
    connect_psql = BashOperator(
        bash_command='prepare.sh',
        env={
            'env_var1':'path/javajdk.jar',
            'env_var2':'path/postgres/'
        }
    )
    #Execute Queries Jujuy and Palermo
    
    

    #Transform Data Jujuy and Palermo
    
    
    
    #Load transformed data to AWS.S3
    load_jujuy = S3CreateObjectOperator(
        aws_conn_id='',
        s3_key='',
        data='',
        replace=True
    )
    load_palermo = S3CreateObjectOperator(
        aws_conn_id='',
        s3_key='',
        data='',
        replace=True
    )

    connect_psql.set_downstream(query_jujuy, query_palermo)
    query_jujuy.set_downstream(transform_jujuy)
    query_palermo.set_downstream(transform_palermo)
    transform_jujuy.set_downstream(load_jujuy)
    transform_palermo.set_downstream(load_palermo)
    --------------------------------------------
    connect_sql >> [query_jujuy, query_palermo] >> [transform_jujuy,transform_palermo] >> [load_jujuy, load_palermo]

    '''
    query