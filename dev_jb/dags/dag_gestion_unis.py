from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def_args = {
    'owner': 'jeremy',
    'depends_on_past': False,
    'start_date': '2022-18-09',
    'end_date': '2022-18-10',
    'catchup': False,
    'email': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes = 2)
}

with DAG(
    'ETL_unis_flores_villa_maria',
    default_args = def_args,
    description = 'ETL para recopilacion de datos sobre estudiantes de las universidades de Flores y Villa Maria',
    schedule_interval = '0 * * * *',
) as dag:
    op = PythonOperator(task_id = 'task_01')

"""
La procesamiento de datos para cada universidad sera independiente uno de la otra, para evitar que un error en la
primer universidad afecte a la segunda.

. operador 1: ejecuta las queries SQL y las guarda en archivos .csv separados.

Los siguentes seran duplicados y ejecutados de manera simultanea para cada univerdiad.
. Operador 2: procesa los datos de la universidad con pandas.
. Operador 3: carga los datos procesados a los servidores de AWS en S3
"""
