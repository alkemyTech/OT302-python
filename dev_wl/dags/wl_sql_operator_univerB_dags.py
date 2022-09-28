from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from functions.load_csv import load_csv


def _put_csv(engine, usr, password, host, port, database ):

    load_csv(engine, usr, password, host, port, database)



with DAG(
    dag_id='wl_sql_operator_univerB_dags',
    start_date= datetime(2021, 1, 1),
    schedule_interval= '@daily',
    catchup= False,
    default_args={
        'dependes_on_past': False,
        'retry_delay': timedelta(hours= 1),
        'retries': 5,   
    },
    description= 'Make an ETL for two different universities',
    tags=['OT302-46'],
) as dag:

        put_csv = PythonOperator(
            task_id="put_csv",
            python_callable= _put_csv,
            op_kwargs=Variable.get("db_settings", deserialize_json=True)
        )

put_csv