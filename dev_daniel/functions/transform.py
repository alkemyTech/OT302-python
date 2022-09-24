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

    df["full_name"] = df["first_name"]
    df["first_name"] = df["full_name"].apply(lambda x: x.split()[0])
    df["last_name"] = df["full_name"].apply(lambda x: x.split()[1])

    df.drop(columns="full_name",inplace=True)

    df.to_csv('data/moron_trans.csv',encoding='utf-8')

#---------------------------------------------------------------

def transform_uni_rio_cuarto():
    pass

#---------------------------------------------------------------

def transform():
    transform_uni_moron('data/Universidad de morón.csv')

#---------------------------------------------------------------
