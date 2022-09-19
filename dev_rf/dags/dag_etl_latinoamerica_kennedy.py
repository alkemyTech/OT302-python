from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta

default_args = {"owner": "rf", "retries": 5, "retry_delta": timedelta(minutes=2)}


def sql_queries():
    """This function is going to use a Postgres Hook to perform the SQL queries to the db"""


def pandas_processing():
    """This function is going to do the pandas processing of the SQL queries"""


def s3_loading():
    """This function is going to have an S3 Hook to load the txt files to S3"""


with DAG(
    dag_id="alkemy_latinoamericana_kennedy",
    default_args=default_args,
    start_date=datetime(2022, 9, 19),
    schedule_interval="0 * * * *",
) as dag:
    pass

    # 1 - Task to perform the SQL Queries
    queries = PythonOperator(task_id="sql-queries", python_callable=sql_queries)

    # 2 - Task to perform the pandas processing
    pandas = PythonOperator(task_id="pandas-processing", python_callable=pandas_processing)

    # 3 - Task to load the txt files into S3
    s3_load = PythonOperator(task_id="S3-loading", python_callable=s3_loading)

    queries >> pandas >> s3_load
