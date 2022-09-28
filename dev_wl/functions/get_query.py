"""
This function Reads the file with the SQLs and splits them into a list
"""
from pathlib import Path
from typing import Optional


def get_query(filename:Optional[str])-> list:
    """ Reads the file with the SQLs and splits them into a list.

    Raises:
        ex: possible SQL file not read error

    Returns:
        list: list with the SQLs
    """
    root = Path.cwd()
    path_file = Path(root / 'dags' / 'scripts' / filename)
    
    try:
        with open(path_file, "r") as file_sql:
            list_sqls = str(file_sql.read()).split(';')
            return list_sqls           
    except Exception as ex:
            print(ex)
            raise ex

