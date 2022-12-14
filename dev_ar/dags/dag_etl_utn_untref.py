# Datetime modules
from datetime import (
    datetime,
    timedelta
    )

# DAG Object
from airflow import DAG

# Airflow Operators
# from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Functions
from functions.utils import (
    extract_from_sql,
    logger,
    transform_universities,
    load_S3
    )

# Logger Config
# One for each university - Log at DAG
# 
logger_untref = logger(logger_name = 'untref')
logger_utn = logger(logger_name = 'utn')

# Loggers
logger_untref.info('DAG Initialized')
logger_utn.info('DAG Initialized')

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
    description = 'ETL for UTN/UNTREF Universities for S3 loading',
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
    sql_queries = PythonOperator(
        task_id = 'sql_queries',
        #Add retries arg at operator level
        #Will retry 5 times just in the sql queries and not other tasks
        retries = 5,
        python_callable = extract_from_sql,
        op_kwargs = {'sql_file_name' : 'uni_utn_untref'}
    )

    # Transform task
    # Operator to transform data using pandas
    transform_pandas = PythonOperator(
        task_id = 'transform_pandas',
        # Calls function 
        python_callable = transform_universities
    )

    # Load task 1 OT302-75
    # Operator to load transformed data into AWS S3
    # Later make a for loop for tasks
    load_s3_file1 = PythonOperator(
        task_id = 'load_s3_file1',
        # Calls S3 Hook Function
        python_callable = load_S3,
        # For another connection id add 'aws_conn_id' : 'new_id_connection' in following op_kwargs parameter
        op_kwargs = {'load_S3_file' : '0_uni_utn_untref'}
    )

    # Load task 2 OT302-76
    # Operator to load transformed data into AWS S3
    load_s3_file2 = PythonOperator(
        task_id = 'load_s3_file2',
        # Calls S3 Hook Function
        python_callable = load_S3,
        # For another connection id add 'aws_conn_id' : 'new_id_connection' in following op_kwargs parameter
        op_kwargs = {'load_S3_file' : '1_uni_utn_untref'}
    )

    #Graph structure
    sql_queries >> transform_pandas >> [load_s3_file1, load_s3_file2]