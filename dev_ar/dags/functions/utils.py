# Functions use in DAGs

# Modules
import logging
import csv
import os
from pathlib import Path
from decouple import RepositoryEnv, Config
from sqlalchemy import create_engine

# Functions
# Logger Function OT302-40
def logger(
    logger_name = 'test',
    logger_format = '%(asctime)s - %(name)s - %(message)s',
    logger_datefmt = '%Y-%m-%d',
    logger_file_path = 'logs',
    logger_file_name = 'logs'
):
    """
    Create and return logger using loggin module with parameters passed
    Set to INFO level
    Use Example: any_logger = logger(logger_name = 'any logger', logger_file_name = 'any_logs') // any_logger.info('Message')
    Args:
        logger_name (str, optional): Logger name. Defaults to 'test'.
        logger_format (str, optional): Log format. Defaults to '%(asctime)s - %(name)s - %(message)s'.
        logger_datefmt (str, optional): Format time in datetime format. Defaults to '%Y-%m-%d'.
        logger_file_path (str, optional): Relative path name. Defaults to 'logs'.
        logger_file_name (str, optional): File name. Defaults to 'logs'
    Returns:
        (logging.logger) : configured logging logger object
    """
    # Create Logger
    custom_logger = logging.getLogger(logger_name)
    # Set Level
    custom_logger.setLevel(logging.INFO)
    # Check if dir exists
    my_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent
    logger_file_path = Path(my_dir, logger_file_path)
    if not Path(logger_file_path).exists():
        Path(logger_file_path).mkdir(parents = False, exist_ok = False)
    # Create File Handler
    # (also can be used StreamHandler() with sys.stdout)
    file_handler = logging.FileHandler(Path(logger_file_path, logger_file_name).with_suffix('.txt'))
    # Create Formatter and add It to Handler
    custom_formatter = logging.Formatter(
        fmt = logger_format,
        datefmt = logger_datefmt
        )
    file_handler.setFormatter(custom_formatter)
    # Add File Handler to Looger
    custom_logger.addHandler(file_handler)
    # Return Logger
    return custom_logger

# Extract Function OT302-48
def extract_from_sql(
    sql_file_name,
    csv_path = './data'
):
    """
    Extracts info from database using SQL sentences from SQL file and creates CSV files with each query
    Uses get_db_settings() and get_sql_queries() aux functions.
    Columns names from SQL table in CSV header
    Args
        sql_file_name (str): SQL file path name
        csv_path (str, optional) : Relative path name
    Returns:
        (None): Creates CSV file
    """
    # Engine set up with settings from external file
    engine_string = 'postgresql://{p[0]}:{p[1]}@{p[2]}:{p[3]}/{p[4]}'.format(p = get_db_settings())
    engine = create_engine(engine_string)
    # Raw Connection and cursor
    connection = engine.raw_connection()
    # Get SQL queries using ext function
    queries = get_sql_queries(sql_file_name = sql_file_name)
    # Add later except Exception block
    try:
        cursor = connection.cursor()
        for n, q in enumerate(queries):
            cursor.execute(q)
            # Get columns names from cursor
            columns = [col[0] for col in cursor.description]
            raw_data = cursor.fetchall()
            my_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent
            csv_file_name = Path(my_dir, csv_path, f'{n}_{sql_file_name}').with_suffix('.csv')
            with open(csv_file_name, 'w') as f:
                csv_writer = csv.writer(f, delimiter = ',')
                # First write columns name then data
                csv_writer.writerow(columns)
                csv_writer.writerows(raw_data)
    finally:
        cursor.close()
        connection.close()

# Aux Functions
# Get Settings
def get_db_settings():
    """
    Get Engine DB Settings from .ini file 
    Returns:
        (list): settings in proper format order 
    """
    my_dir = os.path.dirname(os.path.abspath(__file__))
    SETTINGS_FILE = os.path.join(my_dir, 'settings.ini')
    env_config = Config(RepositoryEnv(SETTINGS_FILE))
    db_settings = [
        env_config.get('DB_USER'),
        env_config.get('DB_PASSWORD'),
        env_config.get('DB_HOST'),
        env_config.get('DB_PORT', cast = int),
        env_config.get('DB_NAME')
    ]
    return db_settings

# Get query from sql file
def get_sql_queries(
    sql_file_name,
    sql_path = './scripts'
):
    """
    Returns list of queries in string format from SQL file.
    Splits sql sentences using ';' char and removes 0 length strings
    Args:
        sql_file_name (string): file name
        sql_path (str, optional) : Relative path name
    Returns:
        (list): SQL queries
    """
    # sql_file_name = './scripts/uni_utn_untref.sql'
    my_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent
    sql_file_name = Path(my_dir, sql_path, sql_file_name).with_suffix('.sql')
    with open(sql_file_name, 'r') as f:
        sql_query = f.read()
    # Use of list comprehension for removing zero lenght strings
    return [e for e in sql_query.split(';') if len(e) > 0]