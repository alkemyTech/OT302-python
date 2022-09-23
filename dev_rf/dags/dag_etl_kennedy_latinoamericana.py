from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta

from functions.sql_queries import sql_queries
from functions.pandas_processing import pandas_processing

# 5 retries for the tasks
default_args = {"owner": "rf", "retries": 5, "retry_delta": timedelta(minutes=2)}


def s3_loading():
    """This function is going to have an S3 Hook to load the txt files to S3"""


with DAG(
    dag_id="dag_etl_kennedy_latinoamericana",
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
            "path_to_scripts_docker": "/opt/airflow/dags/scripts/",
            "path_to_data_docker": "/opt/airflow/dags/data/",
            "sql_file_name": "sql_univ_kennedy_latinoamericana",
            "airflow_connection_id": "postgres_alkemy",
        },
    )

    # 2 - Task to perform the pandas processing
    pandas = PythonOperator(
        task_id="pandas-processing",
        python_callable=pandas_processing,
        op_kwargs={
            "path_to_data_docker": "/opt/airflow/dags/data/",
            "univ_kennedy_file_name": "universidad j. f. kennedy",
            "facu_latinoamericana_file_name": "facultad latinoamericana de ciencias sociales",
        },
    )

    # 3 - Task to load the txt files into S3
    s3_load = PythonOperator(task_id="S3-loading", python_callable=s3_loading)

    queries >> pandas >> s3_load
