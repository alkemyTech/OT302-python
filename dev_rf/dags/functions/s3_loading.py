from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from functions.logging_config import CustomLogger


def s3_loading(path_to_data_docker, filename, airflow_connection_id):
    """Function to load a txt file to S3

    Parameters
    ----------
        path_to_data_docker : str
            path used in docker to access to the data folder
        filename : str
            filename of the file to load to s3. It is going to be uploaded with the same name
        airflow_connection_id : str
            The connection id created in the Airflow dashboard (Admin->Connections) to connect to AWS S3

    Returns
    -------
        loads the txt file to AWS S3
    """

    # Create logger
    if filename == "universidad j. f. kennedy":
        logger = CustomLogger(logger_name="kennedy", file_name=filename)
    elif filename == "facultad latinoamericana de ciencias sociales":
        logger = CustomLogger(logger_name="latinoamericana", file_name=filename)

    # Create hook to S3
    try:
        s3_hook = S3Hook(aws_conn_id=airflow_connection_id)
    except:
        logger.error("Error: Check that you correctly setup the connection to AWS S3")

    # Load the file to S3
    try:
        s3_hook.load_file(
            filename=f"{path_to_data_docker}{filename}.txt",
            key=f"DA-302/{filename}.txt",  # Key is the filename that is going to be saved in the S3
            bucket_name="cohorte-septiembre-5efe33c6",
            replace=True,
        )
        logger.info(f"The file '{filename}.txt' was correctly loaded to S3")
    except:
        logger.error(f"Error: The file '{filename}.txt' could not be loaded to S3")
