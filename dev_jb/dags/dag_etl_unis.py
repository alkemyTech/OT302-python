import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from functions.ejecucion_sql import sql_a_csv
from functions.procesamiento_datos import procesmiento_datos

# Parametros a personalizados #
def_args = {
    'owner': 'jeremy',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 9, 18, tz="UTC"),
    'end_date': pendulum.datetime(2022, 9, 30, tz="UTC"),
    'schedule_interval': '0 * * * *',
    'catchup': False,
    'email': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes = 2)
}

# DAG #
with DAG(
    'ETL_unis_flores_villa_maria',
    default_args = def_args,
    description = 'ETL para recopilacion de datos sobre estudiantes de las universidades de Flores y Villa Maria',
) as dag:
    # Operadores #
    exportacion_datos = PythonOperator(
        task_id = 'task_01-exportacion_de_datos_al_local',
        python_callable = sql_a_csv,
        op_args = ['/c/Users/Jeremy/airflow/dags/scripts',
                   'queries_extraccion_datos_unis.sql',
                   '/c/Users/Jeremy/airflow/dags/data',
                   2]
    )
    procesamiento_de_datos = PythonOperator(
        task_id = 'task_02-procesamiento_de_datos',
        python_callable = procesmiento_datos,
        op_args = ['/c/Users/Jeremy/airflow/dags/data',
                   'universidad_de_flores.csv',
                   'universidad_nacional_de_villa_maría.csv']
    )

    # Ejecucion #
    exportacion_datos >> procesamiento_de_datos
    
"""
La procesamiento de datos para cada universidad sera independiente uno de la otra, para evitar que un error en la
primer universidad afecte a la segunda.

. operador 1: ejecuta las queries SQL y las guarda en archivos .csv separados.

Los siguentes seran duplicados y ejecutados de manera simultanea para cada univerdiad.
. Operador 2: procesa los datos de la universidad con pandas.
. Operador 3: carga los datos procesados a los servidores de AWS en S3
"""
