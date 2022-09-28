# Librerias #
import pandas as pd
from pathlib import Path
from datetime import datetime, date
from functions.logger_conf import logger

# funcion principal #
"""
Funcion: procesa los datos de las universidades segun los requerimientos del cliente y los aplmacena en
archivos .txt en la misma ubicacion de donde se extrajeron.
Args:
    path_datos (str, obligatorio): ruta donde estan almacenados los achivos.csv
    nombre_df1 (str, obligatorio): nombre del primer archivo .csv.
    nombre_df2 (str, obligatorio): nombre del segundo archivo .csv.
    path_df3 (url, opcional): url del archivo de descarga de la tabla de codigos postales. Por defecto esta incluida
    la entregada por el cliente
Uso:
    procesamiento_datos(path_a,
                        nombre_a,
                        nombre_b
    )
"""
def procesmiento_datos(path_datos : str,
                       nombre_df1 : str,
                       nombre_df2 : str,
                       path_df3 = 'https://drive.google.com/uc?export=download&id=1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ'
):
    # Inicilizacion del logger
    logger_flores = logger(Path('/c/Users/Jeremy/airflow/logs'), 'universidad_de_flores.log')
    logger_villa_maria = logger(Path('/c/Users/Jeremy/airflow/logs'), 'universidad_nacional_de_villa_maría.log')
    
    # Log de inicio del proceso
    logger_flores.info('Procesamiento de datos iniciado')
    logger_villa_maria.info('Procesamiento de datos iniciado')
    
    # Lista columnas ordenadas
    columns_after = ['university',
                        'career',
                        'inscription_date',
                        'first_name',
                        'last_name',
                        'gender',
                        'age',
                        'postal_code',
                        'location',
                        'address',
                        'email'
    ]
    
    # Se importan los archivos .csv
    df_cp = df_pc_data(path_df3)
    df_flores = pd.read_csv(Path(path_datos, nombre_df1), encoding = 'utf8', dtype = 'string')
    df_villa_maria = pd.read_csv(Path(path_datos, nombre_df2), encoding = 'utf8', dtype = 'string')
    
    # Preparacion datos Universidad de Villa Maria #
    # Reemplaza los guiones bajos por espacios
    underscore_space_data(df_villa_maria)
    
    # Cambia el formato de fecha
    date_fmt_data(df_villa_maria)
    
    # Hace un join entre la tabla orginial y la tabla de codigos postales
    df_villa_maria = join_data(df_villa_maria, df_cp)
    
    # Preparacion datos Universidad de Flores #
    # Hace un join entre la tabla orginial y la tabla de codigos postales
    df_flores = join_data(df_flores, df_cp)
    
    # Procesamiento general #
    # Se realizan los mismos porcesos para ambas tablas
    for df in [df_flores, df_villa_maria]:
        # Calcula al columna de edad
        df['age'] = df['fecha_nacimiento'].apply(age_data)
        
        # Separa el nombre y el apellido en dos columnas distintas
        name_data(df)
        
        # Reemplaza los valores originales por los solicitados de la columna genero 
        gender_data(df)
        
        # Elimina los guioines
        wipe_dash_data(df)
        
        # Convierte los str de uppercase a lowercase
        lower_data(df)
        
        # Elimina los espacios que esten al inicio y al final del str
        strip_data(df)
        
        # Renombra las columnas con los nombres solicitados
        rename_col_data(df)
    
    # Organiza las columnas en el orden especificado    
    df_flores = df_flores[columns_after]
    df_villa_maria = df_villa_maria[columns_after]
    
    # Guardado de datos en formato .txt en la direccion dada para la universidad de flores
    df_flores.to_csv(Path(path_datos, 'universidad_de_flores.txt'), sep = ',', index = False, encoding = 'utf8')
    
    # Log de guardado de datos
    logger_flores.info(f'Datos guardados correctamente en {path_datos} como archivo .txt, separador "," y codificacion UTF-8')
    
    # Guardado de datos en formato .txt en la direccion dada para la Universidad de villa maria
    df_villa_maria.to_csv(Path(path_datos, 'universidad_nacional_de_villa_maría.txt'), sep = ',', index = False, encoding = 'utf8')
    
    # Log de guardado de datos
    logger_villa_maria.info(f'Datos guardados correctamente en {path_datos} como archivo .txt, separador "," y codificacion UTF-8')
    
    
# Funciones esclavas #

"""
Funcion: elimina los espacios que esten al inicio y al final de las colunas que sean str
Args:
    df (pd.DataFrame, obligatorio): dataframe al que se le aplicara esta funcion
Uso:
    strip_data(df_a)
"""
def strip_data(df : pd.DataFrame
):
    df[df.columns.difference(['age', 'fecha_nacimiento', 'fecha_de_inscripcion'])] = \
    df[df.columns.difference(['age', 'fecha_nacimiento', 'fecha_de_inscripcion'])].applymap(str.strip)

"""
Funcion: cambia de uppercase a lowercase los valores de las columnas que sean str
Args:
    df (pd.DataFrame, obligatorio): dataframe al que se le aplicara esta funcion
Uso:
    lower_data(df)
"""
def lower_data(df : pd.DataFrame
):
    df[df.columns.difference(['age', 'fecha_nacimiento', 'fecha_de_inscripcion'])] = \
    df[df.columns.difference(['age', 'fecha_nacimiento', 'fecha_de_inscripcion'])].applymap(str.lower)

"""
Funcion: elimina los prefijos y sufijos que tenga la columna "nombre" y separa en una columna el nombre y
en otra el apellido
Args:
    df (pd.DataFrame, obligatorio): dataframe al que se le aplicara esta funcion
Uso:
    name_date(df)
"""
def name_data(df : pd.DataFrame
):
    df[['nombre', 'apellido']] = df['nombre'].replace(to_replace = ['MR\. ',
                                                                    'MRS\. ',
                                                                    'DR\. ',
                                                                    'MISS ',
                                                                    'MS\. ',
                                                                    ' MD$',
                                                                    ' DDS$',
                                                                    ' JR.$',
                                                                    ' IV$',
                                                                    ' PHD$',
                                                                    ' DVM$',
                                                                    ' II$',
                                                                    ' III$',
                                                                    ' V$'],
                                                      value = '', regex = True).str.strip().str.split(' ', expand = True)

"""
Funcion: elimina los guiones medios de todas las columnas que sean str
Args:
    df (pd.DataFrame, obligatorio): dataframe al que se le aplicara esta funcion
Uso:
    wipe_dash_data(df)
"""
def wipe_dash_data(df : pd.DataFrame
):
    df[df.columns.difference(['email', 'fecha_nacimiento', 'fecha_de_inscripcion'])] = \
    df[df.columns.difference(['email', 'fecha_nacimiento', 'fecha_de_inscripcion'])].replace('-', '', regex = True)

"""
Funcion: cambia el formato de fecha de las columnas de "fecha_de_inscripcion" y "fecha_nacimiento"
Args:
    df (pd.DataFrame, obligatorio): dataframe al que se le aplicara esta funcion
Uso:
    date_fmt_data(df)
"""
def date_fmt_data(df : pd.DataFrame
):
    df['fecha_de_inscripcion'] = pd.to_datetime(df['fecha_de_inscripcion'])
    df['fecha_de_inscripcion'] = df['fecha_de_inscripcion'].dt.strftime('%Y-%m-%d')
    df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'])
    df['fecha_nacimiento'] = df['fecha_nacimiento'].dt.strftime('%Y-%m-%d')

"""
Funcion: calcula la edad exacta al momento de ejecuacion de la funcion a partir de los datos del dataset
Args:
    nacimeinto (str, obligatorio): fecha en formato string
Uso:
    edad = age_data('2022-12-01')
"""
def age_data(nacimiento : str
):
    nacimiento = datetime.strptime(nacimiento, "%Y-%m-%d").date()
    today = date.today()
    edad = today.year - nacimiento.year - ((today.month, today.day) < (nacimiento.month, nacimiento.day))
    if edad < 0:
        return edad + 100
    else:
        return edad

"""
Funcion: cambia los valores de la columna sexo del dataset
Args:
    df (pd.DataFrame, obligatorio): dataframe al que se le aplicara esta funcion
Uso:
    gemder_data (df)
"""
def gender_data(df : pd.DataFrame
):
    df['sexo'] = df['sexo'].replace({'M' : 'male', 'F' : 'female'})

"""
Funcion: hace el join entre tablas segun si es requerido por codigo postal o localizacion
Args:
    df (pd.DataFrame, obligatorio): dataframe al que se le aplicara esta funcion
    df2 (pd.DataFrame, obligatorio): dataframe al que se le aplicara esta funcion
Uso:
"""
def join_data(df : pd.DataFrame,
              df2 : pd.DataFrame
):
    if 'codigo_postal' in df.columns:
        return df.merge(right = df2, on = 'codigo_postal')
    else: 
        return df.merge(right = df2, on = 'localidad')

"""
Funcion: elimina los guiones bajos de las columnas que no sean 'email'
Args:
    df (pd.DataFrame, obligatorio): dataframe al que se le aplicara esta funcion
Uso:
    underscore_space_data(df)
"""
def underscore_space_data(df : pd.DataFrame
):
    df[df.columns.difference(['email'])] = df[df.columns.difference(['email'])].replace('_', ' ', regex = True)    

"""
Funcion: lee el archivo .csv reerente a los codigos postales y lo almacena en un pandas dataframe
Args:
    path_df3 (str, obligatorio): path o url donde se alamcena el archivo
Uso:
    df = df_pc_data(path_a)
"""
def df_pc_data(path_df3 : str
):
    # Devuelve un DataFrame de pandas con los codigos postales segun localidad
    return pd.read_csv(path_df3, dtype = 'string').drop_duplicates('localidad')

"""
Funcion: elimina las columnas no deseadas y renombra las necesarias con los nombres indicados
Args:
    df (pd.DataFrame, obligatorio): dataframe al que se le aplicara esta funcion
Uso:
    rename_col_data(df)
"""
def rename_col_data(df : pd.DataFrame
):
    columns_new = {'universidad' : 'university',
                   'carrera': 'career',
                   'fecha_de_inscripcion' : 'inscription_date',
                   'nombre' : 'first_name',
                   'apellido' : 'last_name',
                   'sexo' : 'gender',
                   'age' : 'age',
                   'codigo_postal' : 'postal_code',
                   'localidad' : 'location',
                   'direccion' : 'address',
                   'email' : 'email'
}
    df.drop('fecha_nacimiento', axis=1, inplace=True)
    df.rename(columns = columns_new , inplace = True)