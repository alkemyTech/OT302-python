# from email.encoders import encode_noop
# import sys
# sys.path.insert(0, '/root/airflow/dags')

# from airflow import DAG
# from datetime import timedelta, datetime
# from airflow.operators.python import PythonOperator
# from logger import init_logger
# import psycopg2
# import logging
# import csv

# init_logger()

# host = "training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com"
# SQL_PATH = "scripts\extract_info_from_universidad_de_moron.sql"
# user = "alkymer2"
# password="Alkemy23"
# database="training"

# # SQL_PATH:str
# def cargar_sql() -> None:
#     """
#     Ejecuta el archivo .sql pasado por parámetro.
#     Parameters:
#     SQL_PATH: Ubicación del archivo .sql
#     Return:
#     None
#     """
#     query = "SELECT universidad AS university,\
#             carrerra AS career,fechaiscripccion AS inscription_date,\
#             nombrre AS first_name,\
#             sexo AS gender,\
#             nacimiento AS age,\
#             codgoposstal AS postal_code,\
#             eemail AS email \
#             FROM moron_nacional_pampa\
#             WHERE  \
#             (TO_DATE(fechaiscripccion, 'DD/MM/YYYY')\
#             BETWEEN '09-01-2020'\
#             AND '02-01-2021')\
#             AND universidad = 'Universidad de morón';"

#     try:
#         with psycopg2.connect(host=host,
#                                 user=user,
#                                 password=password,
#                                 database=database) as conn:

#             conn.autocommit = True

#             with conn.cursor() as cursor:
#                 try:
#                     cursor.execute(query)
#                     record = cursor.fetchall()
#                     with open("moron.csv", 'w',newline='',encoding="utf-8") as f:
#                         writer = csv.writer(f, delimiter=',')
#                         for elemento in record:
#                             print(elemento)
#                             writer.writerow(elemento)
                        

#                 except (Exception) as error:
#                     logging.error(f"load.py: cargar_sql: 'with open(SQL_PATH)' Error: {error}")
#                     print(f"Error: load.py: cargar_sql: 'with open(SQL_PATH)' Error: {error}")

#     except (Exception) as error:
#         logging.error(f"load.py -> cargar_sql(): 'with psycopg2.connect' Error: {error}")
#         print(f"load.py -> cargar_sql(): 'with psycopg2.connect' Error: {error}")



# default_args = {
# 	'owner' : 'Daniel Casvill',
# 	'depends_on_past' : False,
# 	'email' : ['casvilldaniel@gmail.com'],
# 	'email_on_failure' : False,
# 	'email_on_retry' : False,
# 	'retries' : 5,
# 	'retry_delay' : timedelta(minutes=1)
# }

# with DAG(
#     "dag_universidades",
#     description="Dag para procesar las universidades Universidad De Morón y Universidad Nacional De Río Cuarto",
#     schedule_interval=timedelta(hours=1),
#     start_date=datetime(2022,9,15),
# 	default_args=default_args
# ) as dag:

# 	# extract_task = PythonOperator(task_id= "extract",
# 	# 							  python_callable= cargar_sql)

# 	# transform_task = PythonOperator(task_id= "transform", 
# 	# 								python_callable= transform_file.py)

# 	# load_task = PythonOperator(task_id= "load", 
# 	# 						   python_callable= load_file.py)

# 	# extract_task >> transform_task >> load_task

#     pass

# cargar_sql() 
# logging.info("Fin")

#------------------------------------------------------------------------------------------------------

# import sys
# sys.path.insert(0, '/root/airflow/dags')
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from logger import init_logger
import psycopg2
import logging
import csv

init_logger()

host = "training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com"
SQL_PATH = "scripts\extract_info_from_universidad_de_moron.sql"
user = "alkymer2"
password="Alkemy23"
database="training"



def cargar_sql() -> None:
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
                        print("yes") if (";" in data) else print("not")
                        cursor.execute(data)

                    #Save the result of the sql execution:
                    record = cursor.fetchall()

                    #Open/create 
                    with open("moron.csv", 'w',newline='',encoding="utf-8") as f:
                        writer = csv.writer(f, delimiter=',')

                        for elemento in record:
                            writer.writerow(elemento)


                except (Exception) as error:
                    logging.error(f"load.py: cargar_sql: 'with open(SQL_PATH)' Error: {error}")
                    print(f"Error: load.py: cargar_sql: 'with open(SQL_PATH)' Error: {error}")

    except (Exception) as error:
        logging.error(f"load.py -> cargar_sql(): 'with psycopg2.connect' Error: {error}")
        print(f"load.py -> cargar_sql(): 'with psycopg2.connect' Error: {error}")


cargar_sql()
default_args = {
 'owner' : 'Daniel Casvill',
 'depends_on_past' : False,
 'email' : ['casvilldaniel@gmail.com'],
 'email_on_failure' : False,
 'email_on_retry' : False,
 'retries' : 5,
 'retry_delay' : timedelta(minutes=1)
}

with DAG(
         "dag_universidades",
         description="Dag para procesar las universidades \
                      Universidad De Morón y \
                      Universidad Nacional De Río Cuarto",
         schedule_interval=timedelta(hours=1),
         start_date=datetime(2022,9,15),
         default_args=default_args
        ) as dag:

        extract_task = PythonOperator(task_id= "extract", python_callable= cargar_sql)
        test_task = BashOperator(task_id= "test", bash_command= "pwd")
        extract_task >> test_task

cargar_sql()