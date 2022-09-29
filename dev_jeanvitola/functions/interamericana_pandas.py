from datetime import datetime, date
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


def file_inter(inter):
    df2 = pd.read_csv(inter, sep=",", header=0, encoding="utf-8")
    df2.columns
    # remove - to columns university
    df2['university'] = df2['university'].str.replace('-', " ")
    # lower
    df2['university'] = df2['university'].str.lower()
    # remove - to columns career
    df2['career'] = df2['career'].str.replace('-', " ")
    # lower
    df2['career'] = df2['career'].str.lower()
    # Inscription_date inscription_date: str %Y-%m-%d format
    df2['inscription_date'] = pd.to_datetime(df2['inscription_date'])
    # to name lower
    df2['name'] = df2['name'].str.lower()
    # to delete caracteres
    to_delete = ["mr.", "mrs.", "ms.", "miss", "dr.", "md", "dds", "ii", "dvm", "jr.", "phd"]
    df2['name'] = df2['name'].str.replace('|'.join(to_delete), "")
    # create firts_name an last_name  columns name
    df2['first_name'] = df2['name'].str.split('-').str[0]
    df2['last_name'] = df2['name'].str.split('-').str[1]
    # drop name columns
    df2.drop('name', axis=1, inplace=True)
    # convert columns gender f as female , m as male
    df2['gender'] = df2['gender'].str.replace('F', "female")
    df2['gender'] = df2['gender'].str.replace('M', "male")
    # if the column age < 0 sum 100
    df2.loc[df2['age'] < 0, 'age'] = df2['age'] + 100
    # copy dataframe inter
    df2_copy = df2.copy()
    # email lower,without space
    df2_copy['email'] = df2_copy['email'].str.lower()
    df2_copy['email'] = df2_copy['email'].str.replace(' ', "")
    # locations out - and lower
    df2_copy['locations'] = df2_copy['locations'].str.replace('-', " ")
    df2_copy['locations'] = df2_copy['locations'].str.lower()
    df2_copy.rename(columns={'locations': 'location'}, inplace=True)
    # data csv
    id_google = "1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ"
    data2 = pd.read_csv(f"https://drive.google.com/uc?id={id_google}")
    # Rename data
    data2.rename(columns={'localidad': 'location'}, inplace=True)
    # lower data
    data2['location'] = data2['location'].str.lower()
    # Inner join df_copy with data
    df2_copy = pd.merge(df2_copy, data2, on='location', how='inner')
    # change column name codigo_postal to postal_code
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
