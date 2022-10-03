import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from datetime import datetime,date


#Function to pampa university
def file_transform(pampa,inter):
    #Pampa university
    df = pd.read_csv(pampa, sep=',', header=0, encoding='utf-8')
    #remove - to columns university
    df['university'] = df['university'].str.replace('-', " ")
    #lower
    df['university'] = df['university'].str.lower()
    #remove - to columns career
    df['career'] = df['career'].str.replace('-', " ")
    #lower
    df['career'] = df['career'].str.lower()
    #Inscription_date inscription_date: str %Y-%m-%d format
    df['inscription_date'] = pd.to_datetime(df['inscription_date'])
    #to name lower
    df['name'] = df['name'].str.lower()
    # to delete caracteres 
    to_delete = ["mr.", "mrs.", "ms.", "miss", "dr.", "md", "dds", "ii", "dvm", "jr.", "phd"]
    df['name'] = df['name'].str.replace('|'.join(to_delete), "")
    #create firts_name an last_name  columns name
    df['first_name'] = df['name'].str.split(' ').str[0]
    df['last_name'] = df['name'].str.split(' ').str[1]
    #drop name columns
    df.drop('name', axis=1, inplace=True)
    # convert columns gender f as female , m as male
    df['gender'] = df['gender'].str.replace('F', "female")
    df['gender'] = df['gender'].str.replace('M', "male")
    #convert colum age in datetime and calculate age
    df['age'] = pd.to_datetime(df['age'])
    df['age']=[date.today().year - x.year for x in df['age']]
    #create copy dataframe df
    df_copy=df.copy()
    #email lower,without space
    df_copy['email'] = df_copy['email'].str.lower()
    df_copy['email'] = df_copy['email'].str.replace(' ', "")
    # csv Join
    id_google ="1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ"
    data=pd.read_csv(f"https://drive.google.com/uc?id={id_google}")
    #renanme codigo_postal to postal_code
    data.rename(columns={'codigo_postal':'postal_code'}, inplace=True)
    #Inner join df_copy with data
    df_copy = pd.merge(df_copy, data, on='postal_code', how='inner')
    #Postal Code
    df_copy['postal_code'] = df_copy['postal_code'].astype(str)
    #rename localidad as location
    df_copy.rename(columns={'localidad':'location'}, inplace=True)
    df_copy["location"]=df_copy["location"].str.lower()
    #reorder column university, career, inscription_date, firts_name, last_name, gender, portal_code, location, email
    df_copy = df_copy[['university',
                       'career',
                       'inscription_date',
                       'first_name',
                       'last_name',
                       'gender',
                       'age',
                       'postal_code',
                       'location',
                       'email']]
    # save csv
    df_copy.to_csv("pampa_clean.txt", index=False)

    #INTERAMERICANA UNIVERSITY

    df2=pd.read_csv(inter, sep=",", header=0,encoding="utf-8")
    df2.columns
    #remove - to columns university
    df2['university'] = df2['university'].str.replace('-', " ")
    #lower
    df2['university'] = df2['university'].str.lower()
    #remove - to columns career
    df2['career'] = df2['career'].str.replace('-', " ")
    #lower
    df2['career'] = df2['career'].str.lower()
    #Inscription_date inscription_date: str %Y-%m-%d format
    df2['inscription_date'] = pd.to_datetime(df2['inscription_date'])
    #to name lower
    df2['name'] = df2['name'].str.lower()
    # to delete caracteres 
    to_delete = ["mr.", "mrs.", "ms.", "miss", "dr.", "md", "dds", "ii", "dvm", "jr.", "phd"]
    df2['name'] = df2['name'].str.replace('|'.join(to_delete), "")
    #create firts_name an last_name  columns name
    df2['first_name'] = df2['name'].str.split('-').str[0]
    df2['last_name'] = df2['name'].str.split('-').str[1]
    #drop name columns
    df2.drop('name', axis=1, inplace=True)
    # convert columns gender f as female , m as male
    df2['gender'] = df2['gender'].str.replace('F', "female")
    df2['gender'] = df2['gender'].str.replace('M', "male")
    # if the column age < 0 sum 100
    df2.loc[df2['age'] < 0, 'age'] = df2['age'] + 100
    #copy dataframe inter
    df2_copy = df2.copy()
    #email lower,without space
    df2_copy['email'] = df2_copy['email'].str.lower()
    df2_copy['email'] = df2_copy['email'].str.replace(' ', "")
    #locations out - and lower
    df2_copy['locations'] = df2_copy['locations'].str.replace('-', " ")
    df2_copy['locations'] = df2_copy['locations'].str.lower()
    df2_copy.rename(columns={'locations': 'location'}, inplace=True)
    #data csv
    #id_google ="1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ"
    data2=pd.read_csv(f"https://drive.google.com/uc?id={id_google}")
    #Rename data
    data2.rename(columns={'localidad':'location'}, inplace=True)
    #lower data
    data2['location'] = data2['location'].str.lower()
    #Inner join df_copy with data
    df2_copy = pd.merge(df2_copy, data2, on='location', how='inner')
    #change column name codigo_postal to postal_code
    df2_copy.rename(columns={'codigo_postal': 'postal_code'}, inplace=True)
    df2_copy.rename(columns={'codigo_postal': 'postal_code'}, inplace=True)
    df2_copy = df2_copy[['university',
                         'career',
                         'inscription_date',
                         'first_name',
                         'last_name',
                         'gender',
                         'age',
                         'postal_code',
                         'location',
                         'email']]
    df2_copy.to_csv("inter_clean.txt", index=False)

