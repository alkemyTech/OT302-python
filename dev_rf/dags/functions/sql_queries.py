from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

import csv
import re
from functions.logging_config import CustomLogger

# Create 2 loggers
logger_kennedy = CustomLogger(logger_name="kennedy", file_name="universidad j. f. kennedy")
logger_latinoamericana = CustomLogger(
    logger_name="latinoamericana", file_name="facultad latinoamericana de ciencias sociales"
)
# Create a list of loggers
loggers = [logger_kennedy, logger_latinoamericana]


def sql_queries(path_sql, airflow_connection_id):
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
    try:
        with open(path_sql, "r") as f:
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
        with open(f"/opt/airflow/dags/data/{name_uni}.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(colnames)
            writer.writerows(rows)

        # Log the event
        for logger in loggers:
            if logger.file_name == name_uni:
                logger.info(f"The csv of {name_uni} is saved")
