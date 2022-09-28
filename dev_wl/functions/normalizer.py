"""
    Function that returns a txt for each of the following universities with normalized data
"""
import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import Optional
from datetime import datetime
from dateutil.relativedelta import relativedelta
from functions.configure_logger import configure_logger


def normalizer(files_csv: Optional[list], path_files: Optional[str]):
    """The files to normalize are read, the columns are transformed according to the specification and the txt files are saved

    Args:
        files_csv (Optional[list]): list with the files to normalize
        path_files (Optional[str]): path where the files to transform are
    """

    # set path root
    root = Path.cwd()

    """ The list of CSV files received as function parameters is processed.
        The information from the CSV file is loaded into a DataFrame that is passed as a parameter to the "proc_file" function, this function returns a DataFrame already normalized and with the requested columns.
        The resulting DataFrame is saved to a txt file
    """ 
    i = 1
    for file in files_csv:
        path_f = Path(root / path_files / file)
        path_txt = Path(root / path_files)
        df_university = pd.read_csv(
            filepath_or_buffer=path_f, sep=",", index_col=None, encoding="utf-8"
        )
        df_university_proc = proc_file(df_university)
        if i == 1:
            file_name = "comahue"
        else:
            file_name = "salvador"
        # dataframe is saved as txt
        df_university_proc.to_csv(f"{path_txt}/{file_name}.txt", sep=",", index=False)
        i = i + 1


def proc_file(df_university: Optional[pd.DataFrame]) -> pd.DataFrame:
    """ All the columns of the DataFrame that is received as a parameter of the function are processed one by one according to the following specifications:
        Final Data:
                    university: lowercase str, no extra spaces, no hyphens
                    career: lowercase str, no extra spaces, no hyphens
                    inscription_date: str %Y-%m-%d format
                    first_name: lowercase str and no spaces, no hyphens
                    last_name: lowercase str and no spaces, no hyphens
                    gender: str choice(male, female)
                    age:int
                    postal_code: str
                    location: lowercase str with no extra spaces, no hyphens
                    email: lowercase str, no extra spaces, no hyphens

    Args:
        df_university (pd.DataFrame): deDataFrame to transformscription_

    Returns:
        pd.DataFreme: returns the transformed DataFrameon_
    """
    # list to save the normalized columns and then build the dataframe
    """
        each element of the list is a "pandas.Serie" containing the normalized information for each column.
        At the end of the processing of the columns, with this list of "pandas.Series"
        the final DataFrame containing each element of the list is assembled.
    """
    list_columns_norm = []
    # the columns of the DataFrame are processed one by one
    for column in df_university.columns:
        if column == "university" or column == "career":
            # university: lowercase str, no extra spaces, no hyphens
            # career: lowercase str, no extra spaces, no hyphens
            data = (
                df_university[column]
                .str.lower()
                .str.strip()
                .str.replace("-", " ")
                .str.replace("_", " ")
            )
            # the normalized column is loaded to the list of "pandas.Series"
            list_columns_norm.append(data)
        elif column == "inscription_date":
            # inscription_date: str %Y-%m-%d format
            # the normalized column is loaded to the list of "pandas.Series"
            list_columns_norm.append(df_university[column])
        elif column == "name":
            # The name column is separated into first and last name.
            # first_name: lowercase str and no spaces, no hyphens
            # last_name: lowercase str and no spaces, no hyphens
            data = (
                df_university[column]
                .str.lower()
                .str.strip()
                .str.replace("-", " ")
                .str.replace("_", " ")
                .str.lstrip('miss')
            )
            """  To process the name column, eliminate prefixes and suffixes and separate the first and last names,
                 the python "re" library is used, a regex expression is created that matches the prefixes, suffixes and the first and last name.
                 This prior to seeing the "Miss" prefix removed from the previous line.
            """
            # preparation of the regular expression for the regex
            pattern = "r(?P<abrev1>^[a-z]+[\.])|(?P<abrev2>^ms|^mrsm|^miss|^mss)\s|(?P<b>md$|dds$|jr\.$|dvm$|phd$)|(?P<first>\w+)\s(?P<last>\w+)"
            pattern_regex = re.compile(pattern, re.IGNORECASE)
            result_regex = data.apply(
                lambda x: None if x is np.NaN else pattern_regex.search(x)
            )
            # separate first name, it is using the information in the group "first" that was matched.
            first_name = result_regex.apply(
                lambda x: x if x is None else x.group("first")
            )
            first_name.name = "first_name"
            # the normalized column is loaded to the list of "pandas.Series"
            list_columns_norm.append(first_name)
            # separate last name, it is using the information in the group "last" that was matched.
            last_name = result_regex.apply(
                lambda x: x if x is None else x.group("last")
            )
            last_name.name = "last_name"
            # the normalized column is loaded to the list of "pandas.Series"
            list_columns_norm.append(last_name)
        elif column == "gender":
            # set as male or female
            data = df_university[column].str.lower()
            mask = data == "f"
            data[mask] = "famele"
            mask = data == "m"
            data[mask] = "male"
            list_columns_norm.append(data)
        elif column == "date_birthday":
            # age is calculated from the date of birth
            date_birthday = df_university[column].apply(
                lambda x: datetime.strptime(x, "%Y-%m-%d")
            )
            # age is put as a string
            age = date_birthday.apply(
                lambda x: int(relativedelta(datetime.now(), x).years)
            )
            # if the resulting age is zero, it is set to 1
            mask = age == 0
            age[mask] = 1
            """ If the calculated age is negative, the year is incorrectly loaded in the source file.
                It is decided to subtract 100 as a form of correction.
            """
            mask = age < 0
            age[mask] = age[mask] + 100
            age.name = "age"
            # the normalized column is loaded to the list of "pandas.Series"
            list_columns_norm.append(age)
        elif column == "email":
            # email lowercase str, no extra spaces, no hyphens
            data = (
                df_university[column]
                .str.lower()
                .str.strip()
                .str.replace("-", " ")
                .str.replace("_", " ")
            )
            # the normalized column is loaded to the list of "pandas.Series"
            list_columns_norm.append(data)
        elif column == "postal_code" or column == "location":
            if column == "postal_code":
                """ If the column to be processed is in the "postal_code", not only do we have to normalize the information,
                    It is necessary to obtain from an file the "locality" to
                    that code. For this the function "find_location_postacode" is used.
                """
                # find locacions
                codigo = df_university[column].apply(lambda x: int(x))
                locations = find_location_postalcode(column, codigo)
                list_columns_norm.append(locations)
                # the postal code is put as a string
                data = df_university[column].apply(lambda x: str(x))
                data.name = "postal_code"
                # the normalized column is loaded to the list of "pandas.Series"
                list_columns_norm.append(data)
            else:
                """ If the column to be processed is in the "location", not only do we have to normalize the information,
                    It is necessary to obtain from an file the "postal code" to
                    that code. For this the function "find_location_postacode" is used.
                """
                # the location lowercase str, no extra spaces, no hyphens
                data = (
                    df_university[column]
                    .str.lower()
                    .str.strip()
                    .str.replace("-", " ")
                    .str.replace("_", " ")
                )
                # the normalized column is loaded to the list of "pandas.Series"
                list_columns_norm.append(data)
                # find postal codes
                postal_codes = find_location_postalcode(column, data)
                # postal_code is put as a string
                data = postal_codes.apply(lambda x: str(x))
                data.name = "postal_code"
                # the normalized column is loaded to the list of "pandas.Series"
                list_columns_norm.append(data)

    # the final DataFrame is created
    """ 
    With this list of "pandas.Series"
    the final DataFrame containing each element of the list is assembled.
    """
    list_result = []
    for i in range(len(list_columns_norm)):
        list_result.append(pd.DataFrame(list_columns_norm[i]))
    df_result = pd.concat(list_result, axis=1)
    return df_result


def find_location_postalcode(
    column: Optional[str], data: Optional[pd.DataFrame]
) -> pd.Series:
    """ It receives the columns "postal_code" or "location" as parameters.
        If it is "postal_code" it looks for the locality.
        If it is "location" it looks for the "postal_code".


    Args:
        column (str): column name, postal_code o location
        data (pd.serie): pd.Series according to column

    Returns:
        pd.serie: pd.Series with the requested data, postal_code o location
    """
    # set path root
    root = Path.cwd()
    
    path_f = Path(root / "data" / "codigos_postales.csv")
    # read the external file with the postal codes and locations for each code.
    # the information is into DataFrame.
    df_cod_loc = pd.read_csv(
        filepath_or_buffer=path_f, sep=",", index_col=None, encoding="utf-8"
    )
    # merge column information with de external file
    if column == "postal_code":
        result = pd.merge(
            df_cod_loc,
            data,
            how="right",
            left_on="codigo_postal",
            right_on="postal_code",
        )
        # return only de column location of dataframe as a result of merge
        # lowercase str with no extra spaces, no hyphens
        result_f = (
            result.localidad.str.lower()
            .str.strip()
            .str.replace("-", " ")
            .str.replace("_", " ")
        )        
        result_f.name = "location"
        return result_f
    else:
        # prepare the DataFreme for correct merge. lowercase str localidad and drop the repeated
        df_cod_loc["local"] = df_cod_loc.localidad.str.lower()
        df_cod_loc.drop_duplicates(subset="localidad", keep="first", inplace=True)
        result = pd.merge(
            df_cod_loc, data, how="right", left_on="local", right_on="location"
        )
        # return only de column postal_code of dataframe as a result of merge
        result_f = result.codigo_postal
        result_f.name = "postal_code"
        return result_f
