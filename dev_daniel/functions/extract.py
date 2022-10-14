import logging
import csv
import psycopg2
import pathlib
import os
from decouple import config
#--------------------------------------------------------------------------------------------
work_path = pathlib.Path().resolve()
#--------------------------------------------------------------------------------------------
host= config('HOST')
user= config('USER')
password= config('PASSWORD')
database= config('DATABASE')
sql_name = config('SQL_NAME')
#--------------------------------------------------------------------------------------------

def __split_sql_file_by_queries(sql_name:str = sql_name) -> list:
    '''
    Splits a sql file into queries and saves them into a python list.

    Parameter:
    sql_name: The name of the sql file. It must contain the relative path.
              By default, it's taken from the .env file.
    
    Returns:
    A list with the query/queries.
    '''
    try:
        #Open the sql file, and read it:
        with open(sql_name,'r',encoding="utf-8") as my_file:
            data = my_file.read()

        #Divide the file by queries
        data = data.split(";")

        return data

    except (Exception) as error:
        logging.error(f"extract.py -> __split_sql_file_by_queries(): 'with open(sql_name...):'. {error}")

#--------------------------------------------------------------------------------------------------

def __execute_query(query:str) -> list:
    """
    Executes a query passed by parameter.
    The sql credentials are taken from the .env file.

    Parameters:
    query: The query to be executed

    Returns:
    record: A list of the data taken from the query execution.
    """ 
    try:
        with psycopg2.connect(host=host,
                              user=user,
                              password=password,
                              database=database) as conn:

            conn.autocommit = True
            try:
                with conn.cursor() as cursor:
                    
                    #Run the first query:
                    cursor.execute(query)

                    #Saves the query result:
                    record = cursor.fetchall()
                    #Save the column names at the beginning of the list
                    record.insert(0, [col[0] for col in cursor.description])
                    
                    return record

            except (Exception) as error:
                logging.error(f"extract.py -> __execute_query(): 'with conn.cursor():'. {error}")

    except (Exception) as error:
        logging.error(f"extract.py -> __execute_query(): 'with psycopg2.connect(...):'. {error}")

#------------------------------------------------------------------------------------------------

def __generate_csv(record:list, dir_name:str = 'data') -> None:
    '''
    Generates a csv file from a list of data.

    Parameters:
    record: The data that will be stored.
    dir_name: Optional. Directory name where the file will be stored.
                        The default directory name is 'data'

    Returns:
    None
    '''

    #Determinates the file name:------------
    u_moron= 'Universidad de morón'
    u_rio_cuarto= 'Universidad-nacional-de-río-cuarto'

    if u_moron in record[1]:
        file_name = u_moron

    elif u_rio_cuarto in record[1]:
        file_name = u_rio_cuarto

    else:
        file_name = "Name not detected"
    #---------------------------------------

    #Creates a folder to save the file:
    os.makedirs(os.path.dirname(f'{work_path}/{dir_name}/'), exist_ok=True)

    try:
        #Creates the file and writes the data to it:
        with open(f"{dir_name}/{file_name}.csv", 'w', newline='', encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=',')
            # writer.writerow(col_names)

            for element in record:
                writer.writerow(element)

    except (Exception) as error:
        logging.error(f"extract.py -> __generate_csv(): 'with open(...):'. {error}")

#-------------------------------------------------------------------------------------------------------

def extract() -> None:
    #Extract the queries from the sql file:
    queries = __split_sql_file_by_queries('scripts/extract_info_university_moron_n_university_rio_cuarto.sql')

    for query in queries:
        if query.strip() != '':
            #Execute the query and store the 
            #result of the execution:
            __generate_csv(__execute_query(query))

#-------------------------------------------------------------------------------------------------------