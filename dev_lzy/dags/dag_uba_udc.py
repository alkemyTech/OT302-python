from datetime import timedelta
import sys
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
sys.path.append('.')
from functions.functions import request_to_csv, decir_hola

import os

#import logging

# Arguments default for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['jhonayquer2201@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
}

# Declaring DAG
with DAG(
        dag_id='dag_uba_udc',
        description='Consultas SQL, procesamiento con pandas y carga de datos a S3 de UBA y UDC',
        default_args=default_args,
        schedule_interval='@hourly',
        start_date= datetime.utcnow(),
        tags=['ETL'],
) as dag:
    # Operator: Query for university UBA and UDC
    query_sql_uba_udc_to_csv = PythonOperator(
        task_id="query_uba_udc",
        python_callable=request_to_csv,
        retries=5,
        op_kwargs={
            "path_sql": os.path.abspath("./scripts/queries_uba_udc.sql"),
            "airflow_connect": "postgres_training_alkemy"
        })
    hola_operator = PythonOperator(
        task_id="hola_prueba",
        python_callable=decir_hola,
        retries=5,
    )
    # # Operator: Transform data of university UBA
    # pandas_transform_uba_udc = PythonOperator(task_id="transform_data_uba_udc",
    #                                           # python_callable=transform_data_uba
    #                                           )
    # # Operator: Save data of university UBA
    # save_s3_uba = PythonOperator(task_id="save_data_s3_uba",
    #                              # python_callable=save_data_uba
    #                              )
    # # Operator: Save data of university UDC
    # save_s3_udc = PythonOperator(task_id="save_data_s3_udc",
    #                              # python_callable=save_data_udc
    #                              )
    #
    # # Flow for Graph
    # query_sql_uba_udc_to_csv >> pandas_transform_uba_udc >> save_s3_uba >> save_s3_uba >> save_s3_udc
    query_sql_uba_udc_to_csv >> hola_operator
