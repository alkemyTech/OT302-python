import logging
import boto3
from botocore.exceptions import ClientError
from decouple import config

secret_key = config('SECRET_KEY')
public_key = config('PUBLIC_KEY')
bucket_name = config('BUCKET_NAME')
region_name = config('REGION_NAME')

#-------------------------------------------------------------------------------------------------------

def __get_connection(service_name= 's3', 
                    aws_access_key_id= public_key, 
                    aws_secret_access_key= secret_key, 
                    region_name= region_name) -> boto3.client:
    '''
    Returns an Amazon's Service client instance.
    By default, the argument values are taken from the .env file.

    Parameters:
    service_name:(string) The name of a service, e.g. 's3' or 'ec2'.
    aws_access_key_id:(string) The access key to use when creating the client.
    aws_secret_access_key:(string) The secret key to use when creating the client.
    region_name:(string) The name of the region associated with the client.

    Returns:
    Service client instance.
    '''
    try:
        client_instance = boto3.client(
                                service_name=service_name,
                                aws_access_key_id= aws_access_key_id,
                                aws_secret_access_key= aws_secret_access_key,
                                region_name= region_name
                                )

    except ClientError as error:
        logging.ERROR(f'load.py -> __get_connection() {error}')

    return client_instance

#-------------------------------------------------------------------------------------------------------

def __upload_file(client_instance,file_name,key,bucket_name= bucket_name) -> None:
    '''
    Uploads a file to an Amazon's Service instance.

    Parameters:
    client_instance: Amazon's Service client instance.
    file_name:(str) The path to the file to upload.
    key: (str) The name of the key to upload to.
    bucket_name: (string) The Bucket's name identifier. 
                          By default, it's taken from the .env file.

    Returns:
    None
    '''
    try:
        client_instance.upload_file(file_name,
                              bucket_name,
                              key
                             )

    except Exception as error:
        logging.ERROR(f'load.py -> __upload_file() {error}')

#-------------------------------------------------------------------------------------------------------

def __list_S3_files(client_instance:boto3.client,bucket:str = bucket_name) -> None:
    '''
    Prints all (up to 1,000) of the objects in a bucket with each request.

    Parameters:
    client_instance: S3 Service client instance.
    bucket: Bucket name to list.
            By default, it's taken from the .env file.

    Returns:
    None
    '''

    try:
        response = client_instance.list_objects_v2(Bucket = bucket)
        list_files = response.get('Contents')

        for file in list_files:
            print(f"file name: {file['Key']}\n")

    except Exception as error:
        logging.ERROR(f'load.py -> __list_S3_files() {error}')

#-------------------------------------------------------------------------------------------------------

def load():
    __upload_file(__get_connection(),'transformed_data/rio_cuarto_clean.txt','u_rio_cuarto_clean.txt')
    __upload_file(__get_connection(),'transformed_data/moron_clean.txt','u_moron_clean.txt')
