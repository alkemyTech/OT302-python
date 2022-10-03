from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

import csv
import re
from functions.logging_config import CustomLogger

# Extra function
from pathlib import Path
from functions.utils import get_abs_path

# Create 2 loggers
logger_kennedy = CustomLogger(logger_name="kennedy", file_name="universidad j. f. kennedy")
logger_latinoamericana = CustomLogger(
    logger_name="latinoamericana", file_name="facultad latinoamericana de ciencias sociales"
)
# Create a list of loggers
loggers = [logger_kennedy, logger_latinoamericana]


def sql_queries(path_to_scripts_docker, path_to_data_docker, sql_file_name, airflow_connection_id):
    """This function uses a Postgres Hook to perform the SQL queries to the db.

    Parameters
    ----------
        path : str
            path to the sql file to query
        airflow_connection_id : str
            The connection id created in the Airflow dashboard (Admin->Connections) to connect to the postgres DB

    Returns
    -------
        csv files with the queries

    """

    for logger in loggers:
        logger.info("Starting sql_queries task")

    # Create a hook and cursor to connect to the DB
    try:
        hook = PostgresHook(postgres_conn_id=airflow_connection_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
    except:
        for logger in loggers:
            logger.error("Error: Check that you correctly setup the connection to the DB")

    # Open sql file and split the queries
    #############################################
    # Add abs_path function file
    path_to_scripts_docker = get_abs_path(path_to_scripts_docker)
    file_name = Path(path_to_scripts_docker, sql_file_name).with_suffix('.sql')
    try:
        # with open(f"{path_to_scripts_docker}{sql_file_name}.sql", "r") as f:
        with open(file_name, "r") as f:
            queries = f.read().split(";")
    except:
        for logger in loggers:
            logger.error("Error Reading the SQL File")

    # Process each sql query
    for query in queries:
        cursor.execute(query)
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        # Get the name of the university from the query and then format it to use as file name
        uni_search = re.search(r"(?<=universidades = )'(.*?)' | (?<=universities = )'(.*?)'", query)
        name_uni = uni_search.group(0).lower().replace("-", " ").replace("'", "").strip()

        # Save the query to the csv file into the Docker directory
        #############################################
        # Add abs_path function file
        path_to_data_docker = get_abs_path(path_to_data_docker)
        uni_name = Path(path_to_data_docker, name_uni).with_suffix('.csv')
        # with open(f"{path_to_data_docker}{name_uni}.csv", "w") as f:
        with open(uni_name, "w") as f:
            writer = csv.writer(f)
            writer.writerow(colnames)
            writer.writerows(rows)

        # Log the event
        for logger in loggers:
            if logger.file_name == name_uni:
                logger.info(f"The csv of {name_uni} is saved")
