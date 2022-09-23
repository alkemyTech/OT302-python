from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    description="Dag para procesar las universidades Universidad De Morón y Universidad Nacional De Río Cuarto",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,9,15),
	default_args=default_args
) as dag:

	# extract_task = PostgresOperator(task_id= "extract", 
	# 								postgres_conn_id="postgres_default", 
	# 								sql= extract_file.sql)

	# transform_task = PythonOperator(task_id= "transform", 
	# 								python_callable= transform_file.py)

	# load_task = PythonOperator(task_id= "load", 
	# 						   python_callable= load_file.py)

	# extract_task >> transform_task >> load_task

    pass
