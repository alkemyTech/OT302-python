from airflow import DAG
# from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

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
    "dag_etl_universities_moron_n_rio_cuarto",
    description="ETL to the universities 'Universidad De Morón' and 'Universidad Nacional De Río Cuarto'",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,9,15),
	default_args=default_args
) as dag:

	# extract_task = PythonOperator(task_id="extract", python_callable=extract)
	# transform_task = PythonOperator(task_id="transform", python_callable=transform)
	# load_task = PythonOperator(task_id="load", python_callable=load)

	# extract_task >> transform_task >> load_task

    pass
