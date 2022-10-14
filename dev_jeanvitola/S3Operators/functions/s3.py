
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

""" 
La función upload_s3 implementa un parámetro hooks de s3 para subir los archivos transformados,
filename : Ruta del archivo
Key : Dirección donde se guardará el archivo
bucket_name : nombre del contenedor de objetos
replace : Reemplaza el archivo (True o False)
gzip : Empaquetar el archivo (True o False)

"""

def upload_s3(connection_id = 'keys_aws'):
    connect_s3 = S3Hook(aws_conn_id = connection_id)
    connect_s3.load_file(
        filename = f"/opt/airflow/dags/functions/Querys/{to_file}",
        key = f"DA-302/{files}",
        bucket_name = 'cohorte-septiembre-5efe33c6',
        replace = True,
        gzip = False,
        )
