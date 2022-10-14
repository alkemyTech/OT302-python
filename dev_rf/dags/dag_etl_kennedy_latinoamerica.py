from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from functions.sql_queries import sql_queries
from functions.pandas_processing import pandas_processing
from functions.s3_loading import s3_loading

# 5 retries for the tasks
default_args = {"owner": "rf", "retries": 5, "retry_delta": timedelta(minutes=2)}

# Performs the tasks hourly
with DAG(
    dag_id="dag_etl_kennedy_latinoamericana",
    default_args=default_args,
    start_date=datetime(2022, 9, 22),
    schedule_interval="0 * * * *",
) as dag:

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

    # 3.a - Task to load the universidad j. f. kennedy txt file into S3
    s3_load_kennedy = PythonOperator(
        task_id="S3-loading_kennedy",
        python_callable=s3_loading,
        op_kwargs={
            "path_to_data_docker": "/opt/airflow/dags/data/",
            "filename": "universidad j. f. kennedy",
            "airflow_connection_id": "alkemy_s3_conn",
        },
    )

    # 3.b - Task to load the facultad latinoamericana de ciencias sociales txt file into S3
    s3_load_latinoamericana = PythonOperator(
        task_id="S3-loading_latinoamericana",
        python_callable=s3_loading,
        op_kwargs={
            "path_to_data_docker": "/opt/airflow/dags/data/",
            "filename": "facultad latinoamericana de ciencias sociales",
            "airflow_connection_id": "alkemy_s3_conn",
        },
    )

    queries >> pandas >> [s3_load_kennedy, s3_load_latinoamericana]
