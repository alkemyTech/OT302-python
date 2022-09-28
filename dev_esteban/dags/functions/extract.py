import sys, os, pandas as pd
from typing import List
from airflow.models.connection import Connection
import pandas as pd


    
def extract(path_csv_dir: str, sql_scripts: List, db_connection: str) -> None:
    con = Connection(
        conn_id=db_connection,
        conn_type='postgres',
    )
    print(con.test_connection(),"\n",con.__repr__(),"\n",con.get_hook(), dir(con.get_hook()))
    with open(sql_scripts[0], mode='r') as file:
        command_jujuy = file.read()
        file.close()
    # df = pd.read_sql(command_jujuy, con.get_hook())
    # print(df)
    
    with open(sql_scripts[1], mode='r') as file:
        command_palermo = file.read()
        file.close()
    
    