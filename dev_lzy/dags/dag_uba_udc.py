from datetime import timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

#import logging

# Arguments default for DAG
default_args = {
    'owner': 'Luis',
    'depends_on_past': False,
    'email': ['jhonayquer2201@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

# Declaring DAG
with DAG(
        dag_id= 'dag_uba_udc',
        description='Consultas SQL, procesamiento con pandas y carga de datos a S3 de UBA y UDC',
        default_args=default_args,
        schedule_interval='@hourly',
        start_date=days_ago(1),
        tags=['ETL'],
) as dag:
    # Operator: Query for university UBA
    query_sql_uba_udc = PythonOperator(task_id="query_uba_udc", 
    # python_callable=get_data_uba
    )
    # Operator: Transform data of university UBA
    pandas_transform_uba_udc = PythonOperator(task_id="transform_data_uba_udc", 
    # python_callable=transform_data_uba
    )
    # Operator: Save data of university UBA
    save_s3_uba = PythonOperator(task_id="save_data_s3_uba", 
    # python_callable=save_data_uba
    )
    # Operator: Save data of university UDC
    save_s3_udc = PythonOperator(task_id="save_data_s3_udc", 
    # python_callable=save_data_udc
    )

    # Flow for Graph
    query_sql_uba_udc >> pandas_transform_uba_udc >> save_s3_uba >> save_s3_uba >> save_s3_udc

