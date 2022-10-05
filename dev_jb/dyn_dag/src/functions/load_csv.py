"""
This function loads the "CSV" files with the information obtained from a database of universities.
"""
import pandas as pd
from pathlib import Path
from functions.get_query import get_query
from functions.get_db import GetDB
from functions.configure_logger import configure_logger


PATH_DATA = "data"


def load_csv(engine, usr, password, host, port, database):
    """Load the "CSV" files with the information obtained from a database of universities,
    for this, will execute the queries that were picked up of the file.sql.
    """
    # set path root
    root = Path.cwd()
        
    # the two loggers are configured
    comahue_logger = configure_logger('comahue')
    salvador_logger = configure_logger('salvador')
    comahue_logger.info('Starting to run the task in the DAG.')
    salvador_logger.info('Starting to run the task in the DAG.')
    # Connecting to database
    try:
        db_alkemy = GetDB(engine, usr, password, host, port, database)
        engine = db_alkemy.get_connection()
        comahue_logger.info(
            f"Connecting to database."
        )
        salvador_logger.info(
            f"Connecting to database."
        )
    except Exception as ex:
        comahue_logger.error(
            f"Error connecting to database: {ex}"
        )
        salvador_logger.error(
            f"Error connecting to database: {ex}"
        )
        raise ex
    # Get queries
    try:
        list_querys = get_query(r'/c/Users/Jeremy/airflow/dyn_dag/src/scripts/wl_universityB.sql')
        comahue_logger.info(
            f"Read wl_universityB.sql"
        )
        salvador_logger.info(
            f"Read wl_universityB.sql"
        )
    except Exception as ex:
        comahue_logger.error(
            f"Error to read wl_universityB.sql: {ex}"
        )
        salvador_logger.error(
            f"Error to read wl_universityB.sql: {ex}"
        )
        raise ex
    i = 1
    # the queries are executed and the files are generated
    for query in list_querys:
        try:
            file_name = Path(root / PATH_DATA / f"university-{i}.csv")
            df_university = pd.read_sql(query, con=engine)
            df_university.to_csv(
                file_name, sep=",", encoding="utf-8", index=False
            )
            if (i == 1):
                comahue_logger.info(
                    f"Save CSV"
                )
            else:
                salvador_logger.info(
                    f"Save CSV"
                )
        except Exception as ex:
            if (i == 1):
                comahue_logger.error(
                    f"Error to save CSV: {ex}"
                )
            else:
                salvador_logger.info(
                    f"Error to save CSV: {ex}"
                )
        i = i + 1