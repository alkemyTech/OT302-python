"""
   This Dag run python function to uploads a file to the Bucket in S3

"""
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from functions.s3_upload_file import s3_upload_file


def _upload_txt(file_name, bucket, my_key, secret_access_key, region_name):
    """ this function calls the python script to upload the file to the S3 backet.

    Args:
        file_name (str): name of the file to upload
        bucket (str): S3 parameter
        my_key (str): S3 parameter
        secret_access_key (str): S3 parameter
        region_name (str): S3 parameter
    """
    s3_upload_file(file_name, bucket, my_key, secret_access_key, region_name)


with DAG(
    dag_id='wl_s3_upload_comahue_dags',
    start_date= datetime(2021, 1, 1),
    schedule_interval= '@daily',
    catchup= False,
    default_args={
        'dependes_on_past': False,
        'retry_delay': timedelta(hours= 1),
        'retries': 5,   
    },
    description= 'upload file comahue to S3 bucket',
    tags=['OT302-71'],
) as dag:

        upload_txt = PythonOperator(
            task_id="upload_txt",
            python_callable= _upload_txt,
            op_kwargs=Variable.get("s3_settings", deserialize_json=True)
        )
        
upload_txt