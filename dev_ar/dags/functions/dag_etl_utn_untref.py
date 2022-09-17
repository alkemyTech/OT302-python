# Datetime modules
from datetime import datetime, timedelta

# DAG Object
from airflow import DAG

# Airflow Operators
from airflow.operators.dummy import DummyOperator

# Arguments pass on each operator
default_args = {
    'owner' : 'dev_ar',
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 1)
}

# DAG
with DAG(
    dag_id = 'dag_etl_utn_untref',
    default_args = default_args,
    description = 'ETL Consulta a UTN/UNTREF para carga en S3',
    start_date = datetime(2022, 9, 16),
    # Use datetime.timedelta also can be used crontab
    schedule_interval = timedelta(hours = 1),
    catchup = False
) as dag:
    ## Initial Task
    #initial_op = DummyOperator(
    #    task_id = 'initial_operation'
    #    #DummyOperator just in case any init proceess needed
    #)

    # Extract task
    # Operator to perform sql queries on each university
    sql_queries = DummyOperator(
        task_id = 'sql_queries',
        #Add retries arg at operator level
        #Will retry 5 times just in the sql queries and not other tasks
        retries = 5
        #To be replaced with PythonOperator to make SQL queries
    )

    # Transform task
    # Operator to transform data using pandas
    transform_pandas = DummyOperator(
        task_id = 'transform_pandas'
        #Replace with PythonOperator to import pandas for data transformation
    )

    # Load task
    # Operator to load transformed data into AWS S3
    load_S3 = DummyOperator(
        task_id = 'load_s3'
        #
    )

    #Graph structure
    sql_queries >> transform_pandas >> load_S3