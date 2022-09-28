
#Dag SQL with PythonOeprator

from datetime import datetime, timedelta
from airflow import DAG  
from airflow.operators.python import PythonOperator
from functions.query import get_connection
from functions.transform import file_transform
from functions.s3 import upload_s3


#Arguments 
args= {
  'email': ['jeanvitola@gmail.com'],
  'retries': 5,
  'retry_delay': timedelta(minutes=2)
}

#Instance DAG
dag = DAG(
    dag_id='Extract_Querys',
    default_args=args,
    description='execute querys University',
    schedule_interval='0 * * * *',
    start_date=datetime(2022,9,27),
    catchup=False,
    tags=['Alkemy'] 
    
)

t1 = PythonOperator(
    task_id='Extract_data',
    python_callable=get_connection,
    op_kwargs={},
    dag=dag,
)


t2 = PythonOperator(
    task_id='transform_data',
    python_callable=file_transform,
    op_kwargs={
      "pampa": "/opt/airflow/dags/functions/Querys/files/pampa.csv",
      "inter": "/opt/airflow/dags/functions/Querys/files/inter.csv"
      },
    dag=dag,
)

t3 = PythonOperator(
  task_id = "Upload_S3",
  python_callable= upload_s3,
  op_kwargs={},
  dag=dag,
)



t1 >> t2 >> t3