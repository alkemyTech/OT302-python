import logging


def init_logger(log_name:str = 'default_name') -> None:
    '''
    Use it to do a basic logging configuration

    Parameters:
    log_name: The name of the logger. The default name is 'default_name'

    Returns:
    None
    '''

    log_format = f'%(asctime)s - {log_name} -  %(message)s'

    logging.basicConfig(encoding='utf-8', 
                        level=logging.DEBUG,
                        format= log_format, 
                        datefmt= '%Y-%m-%d')


# def log_dag_uni_moron():
#     return logging.info("DAG etl 'Universidad De Morón' has started")

# def log_dag_uni_rio_cuarto():
#     return logging.info("DAG etl 'Universidad Nacional De Río Cuarto' has started")


# init_logger()
# log_dag_uni_moron()
# log_dag_uni_rio_cuarto()
