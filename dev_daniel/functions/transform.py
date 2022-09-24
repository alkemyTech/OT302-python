import pandas as pd

#---------------------------------------------------------------

def transform_uni_moron(file_name:str) -> None:
    '''
    Transforms data related with the 'Universidad de Morón' from a .csv file.

    Parameters:
    file_name: The name of the file. Must include the relative path.

    Return:
    None
    '''
    df = pd.read_csv(file_name, encoding= 'utf-8')
    print(df.sample(5))

#---------------------------------------------------------------

def transform_uni_rio_cuarto():
    pass

#---------------------------------------------------------------