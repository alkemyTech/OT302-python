""" 
    This function uploads a file to the Bucket in S3

"""
import boto3
from pathlib import Path
from functions.configure_logger import configure_logger

PATH_FILE = "data"
FILE_S3 = "DA-302/"


def s3_upload_file(file_name, bucket, my_key, secret_access_key, region_name):
    """It connects to the Bucket in S3 and uploads the file that it receives as a parameter.

    Args:
        file_name (str): name of the file to upload
        bucket (str): S3 parameter
        my_key (str): S3 parameter
        secret_access_key (str): S3 parameter
        region_name (str): S3 parameter

    Raises:
        ex: connection error
        ex: error loading file
    """

    # the logger is configured
    if (file_name == "comahue"):
        univ_logger = configure_logger("comahue")
    else:
        univ_logger = configure_logger("salvador")
    univ_logger.info(f"Starting to run the task in the DAG for {file_name}")
    root = Path.cwd()
    path_file = Path(root / PATH_FILE)

    try:
        # connect to S3
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=my_key,
            aws_secret_access_key=secret_access_key,
            region_name=region_name,
        )
        univ_logger.info(f"Connecting to S3.")
    except Exception as ex:
        univ_logger.error(f"Error connecting to S3: {ex}")
        raise ex

    try:
        # upload the file to S3
        response = s3_client.upload_file(
            Filename=f"{path_file}/{file_name}.txt",
            Bucket=bucket,
            Key=f"{FILE_S3}{file_name}.txt",
        )
        univ_logger.info(f"Upload {file_name} to S3")
    except Exception as ex:
        univ_logger.error(f"Error to upload {file_name}: {ex}")
        raise ex
