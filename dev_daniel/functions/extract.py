import logging
import csv
import psycopg2
import pathlib
import os
from decouple import config
#---------------------------------------------------------------------
work_path = pathlib.Path().resolve()
#---------------------------------------------------------------------
host= config('HOST')
user= config('USER')
password= config('PASSWORD')
database= config('DATABASE')
sql_name = config('SQL_NAME')
#---------------------------------------------------------------------------------

def generate_csv(file_name:str, record:list, dir_name:str = 'data'):
    '''
    Generates a csv file from a list of data.

    Parameters:
    file_name: The name under which the file will be stored.
               It can contains the relative path.
    record: The data that will be stored.
    dir_name: Optional. Directory name where the file will be stored.
                        The default directory name is 'data'

    Return:
    None
    '''
    os.makedirs(os.path.dirname(f'{work_path}/{dir_name}/'), exist_ok=True)

    with open(f"{dir_name}/{file_name}.csv", 'w', newline='', encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=',')
        for element in record:
            writer.writerow(element)

#-------------------------------------------------------------------------------------

def load_sql(sql_name:str = sql_name) -> None:
    """
    Executes the .sql file passed by parameter.

    Parameters:
    sql_name: Name of the .sql file (Must include the relative path).

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
                    #Open the sql file, and read it:
                    with open(sql_name,'r',encoding="utf-8") as my_file:
                        data = my_file.read()

                    #Divide the data by the queries
                    data = data.split(";")

                    #Run the first query:
                    cursor.execute(data[0])
                    record = cursor.fetchall()
                    generate_csv('Universidad de morón',record)
                    
                    record = ""

                    #Run the second query:
                    cursor.execute(data[1])
                    record = cursor.fetchall()
                    generate_csv('Universidad-nacional-de-río-cuarto',record)


                except (Exception) as error:
                    logging.error(f"extract.py: load_sql(): 'with open(sql_name):'. {error}")


    except (Exception) as error:
        logging.error(f"extract.py -> load_sql(): 'with psycopg2.connect():'. {error}")

#------------------------------------------------------------------------------------------------