# Functions use in DAGs

# Modules
import logging
import csv
import pandas as pd
from pathlib import Path
from decouple import RepositoryEnv, Config
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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
    # my_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent
    my_dir = Path(__file__).resolve().parents[1]
    logger_file_path = Path(my_dir, logger_file_path)
    # if not Path(logger_file_path).exists():
    if not logger_file_path.exists():
        logger_file_path.mkdir(parents = False, exist_ok = False)
    # Create File Handler
    # (also can be used StreamHandler() with sys.stdout)
    # file_handler = logging.FileHandler(Path(logger_file_path, logger_file_name).with_suffix('.txt'))
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

# Extract Functions
# Python Operator OT302-48
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
            my_dir = Path(__file__).resolve().parents[1]
            if not Path(my_dir, csv_path).exists():
                Path(my_dir, csv_path).mkdir(parents = False, exist_ok = False)
            csv_file_name = Path(my_dir, csv_path, f'{n}_{sql_file_name}').with_suffix('.csv')
            with open(csv_file_name, 'w') as f:
                csv_writer = csv.writer(f, delimiter = ',')
                # First write columns name then data
                csv_writer.writerow(columns)
                csv_writer.writerows(raw_data)
    finally:
        cursor.close()
        connection.close()

# Aux Extract Functions
# Get Settings
def get_db_settings():
    """
    Get Engine DB Settings from .ini file 
    Returns:
        (list): settings in proper format order 
    """
    my_dir = Path(__file__).parents[1]
    ################################################################
    # Change filename to settings.ini
    SETTINGS_FILE = Path(my_dir, 'settings_.ini')
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
    my_dir = Path(__file__).resolve().parents[1]
    sql_file_name = Path(my_dir, sql_path, sql_file_name).with_suffix('.sql')
    with open(sql_file_name, 'r') as f:
        sql_query = f.read()
    # Use of list comprehension for removing zero lenght strings
    return [e for e in sql_query.split(';') if len(e) > 0]

# Transform Functions
# General Transform Function used in Python Operator OT302-56
def transform_universities():
    """
    Calls transforming university functions transform_untref() and transform_utn()
    Pass aggregator dictionary with columns and aux functions
    Needed for Python Operator
    Returns:
        (None): Excecute university functions
    """
    transform_settings = {
        'university' : get_norm,
        'careers' : get_norm,
        'inscription_date': get_inc_date,
        'first_name' : get_norm,
        'last_name' : get_norm,
        'gender' : get_gender,
        'age' : get_age,
        'postal_code' : get_norm,
        'location' : get_norm,
        'email' : get_email
        }

    transform_untref(transform_settings = transform_settings)
    transform_utn(transform_settings = transform_settings)

# Functions use for OT302-64
# Function for aggregation 
def transform_untref(transform_settings):
    """
    Reads data from UTN University CSV file using pandas, transform it and saves in txt file format
    Args:
         file_name (str): CSV file name

    Returns:
        (None): Saves txt file with transformed data
    """
    # Custom CSV file name ###### FIX PATH
    file_name = './data/1_uni_utn_untref.csv'
    my_dir = Path(__file__).resolve().parents[1]
    # Read CSV info download from SQL database
    df = pd.read_csv(
        Path(my_dir, file_name),
        dtype = str
        )
    # Aggregate dictionary with custom transform functions
    df = df.agg(transform_settings)
    # Write TXT file with transformed data
    df.to_csv(
        Path(my_dir, file_name).with_suffix('.txt'),
        sep = ',',
        index = False,
        encoding = 'utf8'
        )

# Function for aggregation and joining postal codes
def transform_utn(transform_settings):
    """
    Reads data from UTN University CSV file using pandas, transform it and saves in txt file format
    Args:
         file_name (str): CSV file name

    Returns:
        (None): Saves txt file with transformed data
    """
    # Custom CSV file name
    file_name = './data/0_uni_utn_untref.csv'
    my_dir = Path(__file__).resolve().parents[1]
    # File Path with Postal Code data
    csv_path = r'https://drive.google.com/file/d/1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ/view'
    # Read CSV postal codes info
    postals = pd.read_csv(
        'https://drive.google.com/uc?id={}'.format(csv_path.split('/')[-2]),
        dtype = str
        )
    # Postal codes cleansing
    postals.loc[:, 'localidad'] = postals.loc[:, 'localidad'].str.casefold()
    postals.drop_duplicates(subset = 'localidad', inplace = True)
    # Read CSV info download from SQL database
    df = pd.read_csv(
        Path(my_dir, file_name),
        dtype = str
        )
    # Join both dataframes on postal codes
    merged = pd.merge(
    left = df,
    right = postals,
    left_on = 'postal_code',
    right_on = 'localidad',
    how = 'left',
    )
    # Deleting and renaming postal code columns
    merged = merged.drop(
        columns = ['postal_code', 'localidad']).rename(
            columns = {'codigo_postal' : 'postal_code'})
    # Aggregate dictionaty functions
    merged = merged.agg(transform_settings)
    # Write TXT file with transformed data
    merged.to_csv(
        Path(my_dir, file_name).with_suffix('.txt'),
        sep = ',',
        index = False,
        encoding = 'utf8'
        )

# Aux Functions used by transform_untref and transform_utn
def get_age(date):
    """
    Calculates age given birth date
    Args:
        series (str): date in string format ex '1996-09-15'
    Returns:
        (int): Age in int type
    """
    # Use to_datetime func to set str to datetime type
    date = pd.to_datetime(date)
    # Use relativedelta for year lap issues
    age = relativedelta(pd.Timestamp.now(), date).years 
    return age if age > 0 else age + 100

def get_norm(series):
    """
    Lower, trim/stripand replace underscore from string data in series 
    Args:
        series (pd.series): series of string data to be normalized
    Returns:
        pd.series: series with normalized data
    """
    return series.str.lower().str.strip().replace('_', ' ', regex = True)

def get_gender(gender):
    """
    Gender f/m format to female/male
    Args:
        gender (str): Gender f/m format
    Returns:
        (str): Gender female/male format
    """
    result = 'female' if gender.lower() == 'f' else 'male'
    return result

def get_inc_date(date):
    """
    Converts datetime to "%Y-%m-%d" format
    Args:
        date (str): Date in string format
    Returns:
        (str): date in "%Y-%m-%d" format
    """
    result = pd.to_datetime(date).strftime("%Y-%m-%d")
    return result

def get_email(mail):
    """
    Lower, trim/strip data
    Args:
        mail (pd.series): series of string data to be normalized
    Returns:
        pd.series: series with normalized data
    """
    return mail.str.lower().str.strip()

# Load functions
# Functions used for OT302-75 and OT302-76
def load_S3(
    load_S3_file,
    aws_conn_id = 's3_connection'
    ):
    """
    Function for load task DAG
    Creates an S3 Hook Connection from Airflow Admin Connections
    Load txt files created in Transform functions
    Args:
        load_S3_file (str): list of txt files names without ext.
        aws_conn_id (str): name of S3 admin connection. Defaults to 's3_connection'
    Returns:
        None
    """
    # Use admin connections settings for an S3 Hook
    s3_hook = S3Hook(aws_conn_id = aws_conn_id)
    # Load file method to get txt file into S3 bucket
    s3_hook.load_file(
        # Resolve filepath from __file__ and custom path and txt filename
        filename = Path(Path(__file__).resolve().parents[1], './data/{}.txt'.format(load_S3_file)),
        # Key name and path for loading in bucket
        key = 'DA-302/{}.txt'.format(load_S3_file),
        # Bucket name
        bucket_name = 'cohorte-septiembre-5efe33c6',
        # Replace key if already in bucket
        replace = True
        )

# Aux Pathfile Function
def get_abs_path(rel_path):
    """
    Aux function for
    Args:
        rel_path (_type_): _description_

    Returns:
        _type_: _description_
    """
    dir_base = Path(__file__).resolve().parents[1]
    return Path(dir_base, rel_path)