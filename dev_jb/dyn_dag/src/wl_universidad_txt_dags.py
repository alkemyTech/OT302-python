"""
   Configure the Python Operator to execute the two functions that process the data for the following universities
"""
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from functions.load_csv import load_csv
from functions.normalizer import normalizer


def _put_csv(engine, usr, password, host, port, database ):
    """ Execute the function to generate the csv files with the database information.
    Args:
        engine (_type_): params DB
        usr (_type_): params DB
        password (_type_): params DB
        host (_type_): params DB
        port (_type_): params DB
        database (_type_): params DB
    """
    load_csv(engine, usr, password, host, port, database)


def _put_txt(file_comahue, file_salvador, path_files):
    """ executes the function that normalizes the data of group B universities and saves the txt.
    Args:
        file_comahue (_type_):  files to normalize
        file_salvador (_type_): files to normalize
        path_files (_type_): file path
    """
    normalizer([file_comahue, file_salvador], path_files)


with DAG(
    dag_id='wl_univerB_txt_dags',
    start_date= datetime(2021, 1, 1),
    schedule_interval= '@daily',
    catchup= False,
    default_args={
        'dependes_on_past': False,
        'retry_delay': timedelta(hours= 1),
        'retries': 5,   
    },
    description= 'Normalize data and save txt files',
    tags=['OT302-54'],
) as dag:

        put_csv = PythonOperator(
            task_id="put_csv",
            python_callable= _put_csv,
            op_kwargs=Variable.get("db_settings", deserialize_json=True)
        )
        
        put_txt = PythonOperator(
            task_id="put_txt",
            python_callable= _put_txt,
            op_kwargs=Variable.get("params", deserialize_json=True)
        )

put_csv >> put_txt