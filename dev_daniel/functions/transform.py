import pandas as pd
from datetime import datetime,date
import warnings
warnings.filterwarnings('ignore')


#---------------------------------------------------------------

def normalize(x:str) -> str:
    return x.strip().lower().replace('-',' ')

#---------------------------------------------------------------

def normalize_date(date):
    return pd.to_datetime(date).strftime("%Y-%m-%d")

#---------------------------------------------------------------

def normalize_gender(gender:str) -> str:
    return 'male' if gender == 'M' else 'female'
#---------------------------------------------------------------

def normalize_age_moron(age:str) -> str:
    '''
    '''

    splitted_age = age.split("/")
    age = splitted_age[2] + '-' + splitted_age[1] + '-' + splitted_age[0]
    age = datetime.strptime(age, '%Y-%m-%d').date()

    age = age.strftime("%Y-%m-%d")

    born = datetime.strptime(age, "%Y-%m-%d").date()

    today = date.today()

    age = today.year - born.year - ((today.month, 
                                      today.day) < (born.month, 
                                                    born.day))
    return age if age >= 0 else age + 100
#---------------------------------------------------------------

def normalize_age_rio_cuarto(age:str) -> str:
    '''
    '''

    splitted_age = age.split("/")
    age = splitted_age[2] + '-' + splitted_age[1] + '-' + splitted_age[0]

    age = datetime.strptime(age, '%d-%b-%y').date()
    age = age.strftime("%Y-%m-%d")

    born = datetime.strptime(age, "%Y-%m-%d").date()

    today = date.today()

    age = today.year - born.year - ((today.month, 
                                      today.day) < (born.month, 
                                                    born.day))
    return age if age >= 0 else age + 100

#---------------------------------------------------------------

def clean_full_name(full_name:list) -> list:
    '''
    '''

    for element in full_name:
        if len(element) == 4:
            element.pop(0)
            element.pop()

        elif len(element) == 3:
            if '.' in element[0]:
                element.pop(0)
            else:
                element.pop()
                
    return full_name

#---------------------------------------------------------------

def normalize_name(df,name) -> pd.DataFrame:
    '''
    '''

    df["full_name"] = df["first_name"]
    df["full_name"] = df["full_name"].apply(normalize)


    full_name = df["full_name"].apply(lambda x: x.split())
    full_name = clean_full_name(full_name)
    

    df["first_name"] = full_name.apply(lambda x: x[0])
    df["last_name"] = full_name.apply(lambda x: x[1])

    df.drop(columns="full_name",inplace=True)

    cols = df.columns.to_list()

    cols = cols[:4] + cols[-1:] + cols[4:-1]

    df = df[cols]

    return df

#---------------------------------------------------------------

def transform_uni_moron(file_name:str) -> None:
    '''
    Transforms data related with the 'Universidad de Morón' from a .csv file.

    Parameters:
    file_name: The name of the file. Must include the relative path.

    Returns:
    None
    '''
    df = pd.read_csv(file_name, encoding= 'utf-8')


    df = normalize_name(df,'moron')
    df['university'] = df['university'].apply(normalize)
    df['career'] = df['career'].apply(normalize)
    df['inscription_date'] = df['inscription_date'].apply(normalize_date)
    df['gender'] = df['gender'].apply(normalize_gender)
    df['age'] = df['age'].head(5).apply(normalize_age_moron)
    df['email'] = df['email'].apply(lambda x: x.strip().lower())

    df.to_csv('data/moron_clean.csv',encoding='utf-8',index=True)

#---------------------------------------------------------------

def transform_uni_rio_cuarto(file_name:str) -> None:
    '''
    Transforms data related with the 'Universidad Nacional de Río Cuarto' from a .csv file.

    Parameters:
    file_name: The name of the file. Must include the relative path.

    Returns:
    None
    '''
    df = pd.read_csv(file_name, encoding= 'utf-8')

    df = normalize_name(df,'rio_cuarto')
    df['university'] = df['university'].apply(normalize)
    df['career'] = df['career'].apply(normalize)
    df['location'] = df['location'].apply(normalize)
    df['inscription_date'] = df['inscription_date'].apply(normalize_date)
    df['gender'] = df['gender'].apply(normalize_gender)
    df['age'] = df['age'].apply(normalize_age_rio_cuarto)
    df['email'] = df['email'].apply(lambda x: x.strip().lower())

    df.to_csv('data/rio_cuarto_clean.csv',encoding='utf-8',index=True)

#---------------------------------------------------------------

def transform():
    transform_uni_moron('data/Universidad de morón.csv')
    transform_uni_rio_cuarto('data/Universidad-nacional-de-río-cuarto.csv')

#---------------------------------------------------------------