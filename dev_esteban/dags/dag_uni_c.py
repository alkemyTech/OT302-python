from airflow import DAG

default_args={
    'owner':'dev esteban boada',
}

with DAG(
    dag_id='dagg_uni_c',
    default_args=default_args,
    description='''
    Ejecutar tareas de universidades del grupo C.
        * Universidad de Palermo
        * Universidad Nacional de Jujuy
    '''
) as dag:
    '''
    Operators a implementar:
     
    task1 = bash_operator - Desplegar servidor de postgres de manera local
            airflow.operators.bash.BashOperator
            -> bash_command - direccionar a script .sh

    task2_1 = postgresql_operator - Ejecutar scripts sql para extraer data jujuy_utn
                airflow.providers.postgres.operators.postgres
                -> sql - direccion de sql file
                -> postgres_conn_id - specificar database o conexion
                -> autocommit - ejecuciones de comits sql
                -> retries = 5
                -> retry_delay = timedelta(minutes=5)

    task3_1 = Curar informacion optenida jujuy_utn
                airflow.operators.python.PythonOperator
                -> python_callable - llamar y ejecutar un modulo .py
                -> op_kwargs - dict con argumentos que serian enviados al modulo .py

    task2_2 = postgresql_operator - Ejecutar scripts sql para extraer data uni_palermo
                airflow.providers.postgres.operators.postgres
                -> sql - direccion de sql file
                -> postgres_conn_id - specificar database o conexion
                -> autocommit - ejecuciones de comits sql
                -> retries = 5
                -> retry_delay = timedelta(minutes=5)

    task3_2 = Curar informacion optenida uni_palermo
                airflow.operators.python.PythonOperator
                -> python_callable - llamar y ejecutar un modulo .py
                -> op_kwargs - dict con argumentos que serian enviados al modulo .py
    
    taks4_1 = Cargar data procesada jujuy_utn a S3 AWS.
                S3CreateObjectOperator
                    -> aws_conn_id - id conexion a aws
                    -> s3_key - id del objeto a escribir
                    -> data - data para escrbir
                    -> replace - True o flase reemplazar data

    taks4_2 = Cargar data procesada uni_palermo a S3 AWS.
                S3CreateObjectOperator
                    -> aws_conn_id - id conexion a aws
                    -> s3_key - id del objeto a escribir
                    -> data - data para escrbir
                    -> replace - True o flase reemplazar data

    task1.set_downstream(task2_1, task2_2)
    task2_1.set_downstream(task3_1)
    task2_2.set_downstream(task3_2)
    task3_1.set_downstream(task4_1)
    task3_2.set_downstream(task4_2)
    --------------------------------------------
    task1 >> [task2_1, task2_2] >> [task3_1,task3_2] >> [task4_1, taks4_2]
    '''
    pass