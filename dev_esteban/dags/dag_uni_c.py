#import tools
from airflow import DAG
from datetime import timedelta, datetime

# Import operators
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash import BashOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator


# creating default config for dag
default_args={
    'owner':'dev esteban boada',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

# definning DAG
with DAG(
    dag_id='dag_uni_c_retries',
    default_args=default_args,
    description='''
    Ejecutar tareas de universidades del grupo C.
        * Universidad de Palermo
        * Universidad Nacional de Jujuy
    ''',
    start_date=datetime(2022, 10, 1, 9),
    schedule_interval='@hourly',
    catchup=False

) as dag:
  
# Define task.
    '''
    Operators a implementar:

    #Initi PostgreSQL Server
    connect_psql = BashOperator(
        bash_command='prepare.sh',
        env={
            'env_var1':'path/javajdk.jar',
            'env_var2':'path/postgres/'
        }
    )
    #Execute Queries Jujuy and Palermo
    query_jujuy = PostgresOperator(
        sql='scripts/uni_jujuy.sql',
        postgres_conn_id='postgresql://alkymer2:Alkemy23@199.59.243.222:5432/training?sslmode=verify-ca&sslcert=%2Ftmp%2Fclient-cert.pem&sslkey=%2Ftmp%2Fclient-key.pem&sslrootcert=%2Ftmp%2Fserver-ca.pem',   
        autocommit=True
    )
    transform_jujuy = PythonOperator(
        python_callable='functions/jujuy_transform.py',
        op_kwargs={
            'var1':'in_var1_module',
            'var2':'in_var2_module'
        }
    )

    #Transform Data Jujuy and Palermo
    query_palermo = PostgresOperator(
        sql='scripts/uni_palermo.sql',
        postgres_conn_id='postgresql://alkymer2:Alkemy23@199.59.243.222:5432/training?sslmode=verify-ca&sslcert=%2Ftmp%2Fclient-cert.pem&sslkey=%2Ftmp%2Fclient-key.pem&sslrootcert=%2Ftmp%2Fserver-ca.pem',   
        autocommit=True
    )
    transform_palermo = PythonOperator(
        python_callable='functions/palermo_transform.py',
        op_kwargs={
            'var1':'in_var1_module',
            'var2':'in_var2_module'
        }
    )
    
    #Load transformed data to AWS.S3
    load_jujuy = S3CreateObjectOperator(
        aws_conn_id='',
        s3_key='',
        data='',
        replace=True
    )
    load_palermo = S3CreateObjectOperator(
        aws_conn_id='',
        s3_key='',
        data='',
        replace=True
    )

    connect_psql.set_downstream(query_jujuy, query_palermo)
    query_jujuy.set_downstream(transform_jujuy)
    query_palermo.set_downstream(transform_palermo)
    transform_jujuy.set_downstream(load_jujuy)
    transform_palermo.set_downstream(load_palermo)
    --------------------------------------------
    connect_sql >> [query_jujuy, query_palermo] >> [transform_jujuy,transform_palermo] >> [load_jujuy, load_palermo]

    '''
    pass