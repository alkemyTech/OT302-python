"""
This Class is to manage the database, implements two methods,
one returns the parameters of the database and the other,
the cursor to connect to the database.
"""
from sqlalchemy import create_engine


class GetDB:
    """ Class is to manage the database
    """
    def __init__(self, engine, usr, password, host, port, database) -> None:

        self.engine = engine
        self.usr = usr
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.cursor = ''

    def get_db_parametros(self):
        """Return the parameters of the database
        Returns:
            dict: return a dictionary with the parameters
        """
        return {
            "engine": self.engine,
            "usr": self.usr,
            "password": self.password,
            "host": self.host,
            "port": self.port,
            "database": self.database,
        }

    def get_connection(self):
        """ Database connection
        Returns:
            object: return the cursor to the database
        """
        self.cursor = create_engine(
            f"{self.engine}://{self.usr}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
        return self.cursor