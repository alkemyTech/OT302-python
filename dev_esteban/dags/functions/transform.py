from datetime import datetime, timedelta
from statistics import mode
from numpy import full
import pandas as pd
import os, re


def process_data(path_csv_dir: str):
    palermo_df=pd.read_csv(path_csv_dir+'palermo.csv')
    jujuy_df=pd.read_csv(path_csv_dir+'jujuy.csv')
    _restruct_palermo(path_csv_dir, palermo_df)
    _restruct_jujuy(path_csv_dir, jujuy_df)

def _restruct_palermo(path: str, df: pd.DataFrame)-> None:
    postales = pd.read_csv(path+'codigos_postales.csv')
    final_df = _create_empty_df()

    final_df['university'] = df['universidad'].str.replace(r'^\_','',regex=True)
    final_df['university'] = final_df['university'].str.replace(r'\_',' ',regex=True)
    final_df = final_df.drop(final_df[final_df['university']!='universidad de palermo'].index)

    final_df['career'] = df['careers'].str.replace(r'\_$','',regex=True)
    final_df['career'] = final_df['career'].str.replace(r'\_',' ',regex=True)

    final_df['inscription_date'] = pd.to_datetime(df['fecha_de_inscripcion'],format='%d/%b/%y')
    final_df['inscription_date'] = final_df['inscription_date'].apply(_datetime_str)

    full_name = df['names'].apply(_other_clean,**{'which':'palermo'})
    name_df = full_name.str.split('_', expand=True)
    final_df['first_name'] = name_df[0]
    final_df['last_name'] = name_df[1]

    final_df['gender'] = df['sexo'].str.replace('m','male')
    final_df['gender'] = final_df['gender'].str.replace('f','female')
    

    tmp_birth_date = pd.to_datetime(df['birth_dates'],format='%d/%b/%y')
    final_df['age'] = tmp_birth_date.apply(_find_age, **{'which':'palermo'})
    
    df = df.merge(postales, how='left', on='codigo_postal')
    final_df['postal_code'] = df['codigo_postal'].astype(str)
    final_df['location'] = df['localidad'].str.lower()

    final_df['email'] = df['correos_electronicos']
    
    _write_txt(final_df, path, 'curated_palermo')
    

def _restruct_jujuy(path: str, df: pd.DataFrame):
    postales = pd.read_csv(path+'codigos_postales.csv')

    final_df = _create_empty_df()

    final_df['email'] = df['email']
    
    # Hacer join con postales csv 
    postales.rename(columns={'localidad':'location'}, inplace=True)
    postales['location'] = postales['location'].str.lower()
    df = df.merge(postales, on='location', how='left')
    final_df['location'] = df['location']
    final_df['postal_code'] = df['codigo_postal'].astype(str)

    final_df['age'] = df['age'].astype('Int64')

    final_df['gender'] = df['sexo'].str.replace('m','male') 
    final_df['gender'] = final_df['gender'].str.replace('f','female')

    final_df['first_name'] = df['firstname']
    final_df['last_name'] = df['lastname']

    final_df['inscription_date'] = df['inscription_date']

    final_df['career'] = df['career']

    final_df['university'] = df['university']
    final_df = final_df.drop(final_df[final_df['university']!='universidad nacional de jujuy'].index)
    

    _write_txt(final_df, path,'curated_jujuy')


def _datetime_str(dt: pd.Series):
    #convertir datetime to str
    return datetime.strftime(dt, '%Y-%m-%d')

def _find_age(dt: pd.Series, which: str):
    #Encontrar la edad a partir del año de nacimiento
    if which == 'palermo':
        today = datetime.today()
        delta = today - dt
        days = delta.days//365
        if days < 0:
            days += 100
        return days

def _other_clean(dt: pd.Series, which: str):
    return re.sub(r'[dmsr]*\._|(_[djmrv]*\.?$)', '', dt)

def _write_txt(df: pd.DataFrame, path: str, name: str):

    with open(path+name+'.txt', mode='w') as file:
        file.write(df.to_string(index=False))
        file.close()


def _create_empty_df() -> pd.DataFrame:
    final_df = pd.DataFrame(
            columns=['university',
                    'career',
                    'inscription_date',
                    'first_name',
                    'last_name',
                    'gender',
                    'age',
                    'postal_code',
                    'location',
                    'email']
        )
    return final_df






    