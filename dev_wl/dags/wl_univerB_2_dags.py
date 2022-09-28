from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

extract_load_data = DAG(
    dag_id='wl_univerB_2_dags',
    default_args={
        'dependes_on_past': False,
        'retry_delay': timedelta(hours= 1),
        'retries': 5,  
    },
    description= 'Make an ETL for two different universities',
    schedule_interval= timedelta(days= 1),
    start_date= datetime(2022, 10, 1),
    catchup= False,
    tags=['OT302-22'],
)
    
    

'''
Documentar los operators que se deberían utilizar a futuro, 
teniendo en cuenta que se va a hacer dos consultas SQL (una para cada universidad),
se van a procesar los datos con pandas y se van a cargar los datos en S3.

Set the tasks:

extract_data_load_csv = BashOperator(
        task_id="extract_data_load_csv",
        bash_command="python3 /airflow-docker/OT302-python/dev_wl/functions/sql_to_csv.py ",
        dag= extract_load_data
    )

Setting up Dependencies:

task_1.set_upstream(task_2)

    if __name__ == "__main__":
        dag.cli()

'''