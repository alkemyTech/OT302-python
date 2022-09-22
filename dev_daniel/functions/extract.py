import logging
import csv
import psycopg2

host = "training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com"
SQL_PATH = "extract_info_univerity_moron_n_university_rio_cuarto.sql"
user = "alkymer2"
password="Alkemy23"
database="training"

def generar_csv(file_name,record):
    with open(file_name+".csv", 'w',newline='',encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=',')
        for elemento in record:
            writer.writerow(elemento)

def cargar_sql(SQL_PATH:str) -> None:
    """
    Ejecuta el archivo .sql pasado por parámetro.
    Parameters:
    SQL_PATH: Ubicación del archivo .sql
    Return:
    None
    """

    try:
        with psycopg2.connect(host=host,
                                user=user,
                                password=password,
                                database=database) as conn:

            conn.autocommit = True

            with conn.cursor() as cursor:
                try:
                    #Open de sql file, read it and execute it:
                    with open(SQL_PATH,'r',encoding="utf-8") as my_file:
                        data = my_file.read()
                    data = data.split(";")
                    cursor.execute(data[0])
                    record = cursor.fetchall()
                    generar_csv('Universidad de morón',record)
                    record = ""
                    cursor.execute(data[1])
                    record = cursor.fetchall()
                    generar_csv('Universidad-nacional-de-río-cuarto',record)


                except (Exception) as error:
                    logging.error(f"load.py: cargar_sql: 'with open(SQL_PATH)' Error: {error}")
                    print(f"Error: load.py: cargar_sql: 'with open(SQL_PATH)' Error: {error}")

    except (Exception) as error:
        logging.error(f"load.py -> cargar_sql(): 'with psycopg2.connect' Error: {error}")
        print(f"load.py -> cargar_sql(): 'with psycopg2.connect' Error: {error}")