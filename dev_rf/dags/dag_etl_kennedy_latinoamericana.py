from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta

from functions.sql_queries import sql_queries

# 5 retries for the tasks
default_args = {"owner": "rf", "retries": 5, "retry_delta": timedelta(minutes=2)}


def pandas_processing():
    """This function is going to do the pandas processing of the SQL queries"""


def s3_loading():
    """This function is going to have an S3 Hook to load the txt files to S3"""


with DAG(
    dag_id="alkemy_kennedy_latinoamericana",
    default_args=default_args,
    start_date=datetime(2022, 9, 19),
    schedule_interval="0 * * * *",
) as dag:
    pass

    # 1 - Task to perform the SQL Queries
    queries = PythonOperator(
        task_id="sql-queries",
        python_callable=sql_queries,
        op_kwargs={
            "path_sql": "/opt/airflow/dags/scripts/sql_univ_kennedy_latinoamericana.sql",
            "airflow_connection_id": "postgres_alkemy",
        },
    )

    # 2 - Task to perform the pandas processing
    pandas = PythonOperator(task_id="pandas-processing", python_callable=pandas_processing)

    # 3 - Task to load the txt files into S3
    s3_load = PythonOperator(task_id="S3-loading", python_callable=s3_loading)

    queries >> pandas >> s3_load
