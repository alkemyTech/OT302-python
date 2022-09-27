import pandas as pd
from datetime import datetime,date
import warnings
warnings.filterwarnings('ignore')


#---------------------------------------------------------------

def normalize(x:str) -> str:
    return x.strip().lower().replace('-',' ')

#---------------------------------------------------------------

def normalize_date(date:str):
    return pd.to_datetime(date).strftime("%Y-%m-%d")

#---------------------------------------------------------------

def normalize_gender(gender:str) -> str:
    return 'male' if gender == 'M' else 'female'
#---------------------------------------------------------------

def organize_features(df:pd.DataFrame) -> pd.DataFrame:
    
    cols = ['university',
            'career',
            'inscription_date',
            'first_name',
            'last_name',
            'gender',
            'age',
            'postal_code',
            'location',
            'email']

    return df[cols]

#---------------------------------------------------------------

def normalize_postal_code_location(df:pd.DataFrame,feature:str) -> pd.Series:
    '''
    Uses a csv file up on google drive to extract the feature needed (postal code or location).

    Parameters:
    df: The DataFrame that needs the feature.
    feature: The feature needed

    Returns:
    A dataframe with all the features (Included the new feature)
    '''

    #The postal code / location data:
    d = pd.read_csv('https://drive.google.com/uc?id=1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ')
    

    if feature == 'location':
        return df.merge(d, left_on='postal_code', right_on='codigo_postal')['localidad']
    else:
        d['localidad'] = d['localidad'].apply(normalize)
        return df.merge(d, left_on='location', right_on='localidad')['codigo_postal']

#---------------------------------------------------------------

def normalize_age_moron(age:str) -> int:
    '''
    Calculates the age from a given string date in wich the
    date format is %Y-%m-%d.

    Parameters:
    age: The string date.

    Returns:
    The age calculated.
    '''

    splitted_age = age.split("/")
    age = splitted_age[2] + '-' + splitted_age[1] + '-' + splitted_age[0] # Revisar esta parte

    age = datetime.strptime(age, '%Y-%m-%d').date() #The only difference bettwenn this and the next one function

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
    Calculates the age from a given string date in wich the
    date format is %d-%b-%y.

    Parameters:
    age: The string date.

    Returns:
    The age calculated.
    '''

    splitted_age = age.split("/")
    age = splitted_age[2] + '-' + splitted_age[1] + '-' + splitted_age[0] # Revisar esta parte

    age = datetime.strptime(age, '%d-%b-%y').date() #The only difference bettwenn this and the last one function

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
    Cleans a given name by deleting the suffixes and prefixes

    Parameters:
    full_name: A list with the full name (Including suffixes and prefixes).

    Returns:
    A list with only the name and lastname
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
    df['location'] = normalize_postal_code_location(df,'location')
    df['location'] = df['location'].apply(normalize)
    df['inscription_date'] = df['inscription_date'].apply(normalize_date)
    df['gender'] = df['gender'].apply(normalize_gender)
    df['age'] = df['age'].apply(normalize_age_moron)
    df['email'] = df['email'].apply(lambda x: x.strip().lower())

    df = organize_features(df)

    df.to_csv('data/moron_clean.txt',encoding='utf-8',index=False)

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
    df['postal_code'] = normalize_postal_code_location(df,'postal_code')
    df['inscription_date'] = df['inscription_date'].apply(normalize_date)
    df['gender'] = df['gender'].apply(normalize_gender)
    df['age'] = df['age'].apply(normalize_age_rio_cuarto)
    df['email'] = df['email'].apply(lambda x: x.strip().lower())

    df = organize_features(df)


    df.to_csv('data/rio_cuarto_clean.txt',encoding='utf-8',index=False)

#---------------------------------------------------------------

def transform():
    transform_uni_moron('data/Universidad de morón.csv')
    transform_uni_rio_cuarto('data/Universidad-nacional-de-río-cuarto.csv')

#---------------------------------------------------------------
transform()