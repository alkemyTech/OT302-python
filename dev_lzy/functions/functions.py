from distutils.log import debug
import re
import sys
import pandas as pd
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logging.basicConfig(filename="log_uba_query", level="DEBUG")
# DB_NAME = "training"
# DB_USER = "alkymer2"
# DB_PASSWORD = "Alkemy23"
# DB_HOST = "training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com"
# DB_PORT = "5432"


def request_to_csv(path_sql, airflow_connect):
    logging.info("Ejecutando tarea csv")
    conn = PostgresHook(
        postgres_conn_id=airflow_connect,
        default_conn_name="postgres_alkemy"
    ).get_conn()
    cursor = conn.cursor()
    columns = ["university", "career", "inscription_date", "first_name", "last_name", "email", "gender", "birth_dates",
               "postal_code"]
    names_csv = ["uba_csv", "udc_csv"]

    with open(path_sql, "r") as sql_file:
        sql = sql_file.read()

    for match, name_csv in list(zip(re.finditer(pattern=r"[^;]+;", string=sql), names_csv)):
        group = match.group()
        cursor.execute(group)
        if not re.findall(r"uba_kenedy", group):
            columns[8] = "locations"
        sql_query = cursor.fetchall()
        df = pd.DataFrame(sql_query, columns=columns)
        df.to_csv(os.path.abspath(f"./scripts/{name_csv}.csv"))
        logging.info(os.path.abspath(f"./scripts/{name_csv}.csv"))


def decir_hola():
    logging.info("Ejecutando tarea Hola")
    print("Hola")
