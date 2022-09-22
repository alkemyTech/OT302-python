import pathlib
work_path = pathlib.Path().absolute()

import sys
sys.path.insert(0, fr'{work_path}')

from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from functions.logger import init_logger
from functions.extract import cargar_sql

import logging

SQL_PATH = fr"{work_path}\scripts\extract_info_univerity_moron_n_university_rio_cuarto.sql"

init_logger()


default_args = {
 'owner' : 'Daniel Casvill',
 'depends_on_past' : False,
 'email' : ['casvilldaniel@gmail.com'],
 'email_on_failure' : False,
 'email_on_retry' : False,
 'retries' : 5,
 'retry_delay' : timedelta(minutes=1)
}

with DAG(
         "dag_universidades",
         description="Dag para procesar las universidades \
                      Universidad De Morón y \
                      Universidad Nacional De Río Cuarto",
         schedule_interval=timedelta(hours=1),
         start_date=datetime(2022,9,15),
         default_args=default_args
        ) as dag:


        extract_task = PythonOperator(task_id= "extract", 
                                      python_callable= cargar_sql,
									  op_kwargs={SQL_PATH:SQL_PATH})
        extract_task

cargar_sql(SQL_PATH)