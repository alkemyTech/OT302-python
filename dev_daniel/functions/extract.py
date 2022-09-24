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

def __generate_csv(file_name:str,col_names:list, record:list, dir_name:str = 'data'):
    '''
    Generates a csv file from a list of data.

    Parameters:
    file_name: The name under which the file will be stored.
               It can contains the relative path.
    col_names: 
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
            writer.writerow(col_names)
            writer.writerow(element)

#-------------------------------------------------------------------------------------

def load_sql(sql_name:str = sql_name) -> None:
    """
    Executes the .sql file passed by parameter and
    call the _generate_csv() function to export the data 
    genereted from the .sql file.
    It takes the sql credentials from the .env file.


    Parameters:
    sql_name: Name of the .sql file (Must include the relative path).
              The default arg is taken from the .env file.

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

                    #Catch the column names:
                    col_names = [col[0] for col in cursor.description]

                    #Saves the query result:
                    record = cursor.fetchall()

                    __generate_csv('Universidad de morón',col_names,record)
                    
                    record = ""
                    col_names = []

                    #Run the second query:
                    cursor.execute(data[1])

                    #Catch the column names:
                    col_names = [col[0] for col in cursor.description]

                    #Saves the query result:
                    record = cursor.fetchall()

                    __generate_csv('Universidad-nacional-de-río-cuarto',col_names,record)


                except (Exception) as error:
                    logging.error(f"extract.py: load_sql(): 'with open(sql_name):'. {error}")


    except (Exception) as error:
        logging.error(f"extract.py -> load_sql(): 'with psycopg2.connect():'. {error}")

#------------------------------------------------------------------------------------------------