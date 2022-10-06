import boto3
from functions.logging_setup_c import CustomLogger
from decouple import RepositoryEnv

custom_config = RepositoryEnv('/Users/sergioboada/.airflow/ot302-python/dev_esteban/dags/.env')

bucket = custom_config.data['BUCKET_NAME']
pub_key = custom_config.data['PUBLIC_KEY']
priv_key = custom_config.data['SECRET_KEY']
region = custom_config.data['REGION']

logger = CustomLogger('jujuy','palermo')
jujuy_logger = logger.get_logger('jujuy')
palermo_logger = logger.get_logger('palermo')

def load_jujuy(path_csv_dir: str):    

    try:
        s3_conn= boto3.client(
            "s3",
            aws_access_key_id=pub_key,
            aws_secret_access_key=priv_key,
            region_name=region
        )

        response = s3_conn.upload_file(
            Filename=f"{path_csv_dir}/curated_jujuy.txt",
            Bucket=bucket,
            Key=f"DA-302/universidad_jujuy.txt"
        )
        jujuy_logger.info('Loaded successfully')
    except Exception as e:
        jujuy_logger.critical(f"Load failed. ({e.args})")
        


def load_palermo(path_csv_dir: str):
    try:
        s3_conn= boto3.client(
            "s3",
            aws_access_key_id=pub_key,
            aws_secret_access_key=priv_key,
            region_name=region,
        )

        response = s3_conn.upload_file(
            Filename=f"{path_csv_dir}/curated_palermo.txt",
            Bucket=bucket,
            Key=f"DA-302/universidad_palermo.txt",
        )
        palermo_logger.info('Loaded successfully')
    except Exception as e:
        palermo_logger.critical(f"Load failed. ({e.args})")

def list_bucket_content():
    try:
        s3_conn= boto3.client(
            "s3",
            aws_access_key_id=pub_key,
            aws_secret_access_key=priv_key,
            region_name=region
        )

        response = s3_conn.list_objects_v2(Bucket=bucket)
        list_files = response.get('Contents')
        for i in list_files:
            print(i['Key'])
    except Exception as e:
        print(e)


# path = '/Users/sergioboada/.airflow/ot302-python/dev_esteban/dags'
# op_kwargs={
#             'path_csv_dir': f"{path}/files/"
#         }
# load_jujuy(op_kwargs['path_csv_dir'])
# load_palermo(op_kwargs['path_csv_dir'])