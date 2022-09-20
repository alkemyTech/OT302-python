from airflow import DAG
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
    "dag_universidades",
    description="Dag para procesar las universidades Universidad De Morón y Universidad Nacional De Río Cuarto",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,9,15),
	default_args=default_args
) as dag:


    pass
