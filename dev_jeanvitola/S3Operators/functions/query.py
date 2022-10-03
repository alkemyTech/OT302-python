from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import os
#from decouple import config


def get_connection():

    # credenciales de conexión
    # config('DB_HOST')
    host = "training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com"
    database = "training"  # config('DB_NAME')
    user = "alkymer2"  # config('DB_USER')
    port = 5432
    password = "Alkemy23"  # config('DB_PASSWORD')
    url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
        user, password, host, port, database)

    try:

        url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database)
        conn = psycopg2.connect(url)
        conn.autocommit = True
        print(f"La conexión al {host} con el {user}  ha iniciado.")

    except Exception as ex:
        print("La conexión a la base de datos ha presentado un error: \n", ex)

    # Querys pampa
    # Open and read the file as a single buffer
    os.chdir(r'/opt/airflow/dags/functions/Querys')
    qp = open('pampa_query.sql', 'r')
    sql_pampa = qp.read()
    qp.close()
    # Ejecución de consulta
    query_pampa = pd.read_sql_query(sql_pampa, url)
    print(query_pampa)
    # Save files to csv
    try:
        os.makedirs('./files/')
    except FileExistsError:
        pass
    query_pampa.to_csv('./files/pampa.csv', header=True, index=False)

    # Querys_interamericana
    # Open and read the file as a single buffer
    os.chdir(r'/opt/airflow/dags/functions/Querys')
    fd = open('interamericana.sql', 'r')
    sql_inter = fd.read()
    fd.close()
    # Ejecución de consulta
    query_inter = pd.read_sql_query(sql_inter, url)
    print(query_inter)
    # Save files to csv
    query_inter.to_csv('./files/inter.csv', header=True, index=False)


get_connection()
