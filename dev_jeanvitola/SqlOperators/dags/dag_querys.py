
# Dag SQL with PythonOeprator

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from functions.query import get_connection


# Arguments
args = {
    'email': ['jeanvitola@gmail.com'],
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Instance DAG
dag = DAG(
    dag_id='Extract_Querys',
    default_args=args,
    description='execute querys University',
    schedule_interval='0 * * * *',
    start_date=datetime(2022, 9, 30),
    catchup=False,
    tags=['Alkemy']

)

t1 = PythonOperator(
    task_id='Extract_data',
    python_callable=get_connection,
    op_kwargs={},
    dag=dag,
)


t1
