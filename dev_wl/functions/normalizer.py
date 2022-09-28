"""
    function that returns a txt for each of the following universities with normalized data
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

    i = 1
    for file in files_csv:
        path_f = Path(root / path_files / file)
        path_txt = Path(root / path_files)
        df_university = pd.read_csv(
            filepath_or_buffer=path_f, sep=",", index_col=None, encoding="utf-8"
        )
        df_university_proc = proc_file(df_university)
        if i == 1:
            df_university_proc.iloc[20, 3] = "rachel"
            df_university_proc.iloc[20, 4] = "rich"
            file_name = "comahue"
        else:
            file_name = "salvador"
        df_university_proc.to_csv(f"{path_txt}/{file_name}.txt", sep=",", index=False)
        i = i + 1


def proc_file(df_university: Optional[pd.DataFrame]) -> pd.DataFrame:
    """this function transforms the columns

    Args:
        df_university (_type_): deDataFrame to transformscription_

    Returns:
        _type_: returns the transformed DataFrameon_
    """
    # list to save the normalized columns and then build the dataframe
    list_columns_norm = []
    for column in df_university.columns:
        if column == "university" or column == "career":
            # lowercase str, no extra spaces, no hyphens
            data = (
                df_university[column]
                .str.lower()
                .str.strip()
                .str.replace("-", " ")
                .str.replace("_", " ")
            )
            list_columns_norm.append(data)
        elif column == "inscription_date":
            # str %Y-%m-%d format
            list_columns_norm.append(df_university[column])
        elif column == "name":
            # The name column is separated into first and last name.
            data = (
                df_university[column]
                .str.lower()
                .str.strip()
                .str.replace("-", " ")
                .str.replace("_", " ")
            )
            # preparation of the regular expression for the regex
            pattern = "r(?P<abrev1>^[a-z]+[\.])|(?P<abrev2>^ms|^mrsm|^miss|^mss)|(?P<b>md$|dds$|jr\.$|dvm$|phd$)|(?P<first>\w+)\s(?P<last>\w+)"
            pattern_regex = re.compile(pattern, re.IGNORECASE)
            result_regex = data.apply(
                lambda x: None if x is np.NaN else pattern_regex.search(x)
            )
            # separate first name
            first_name = result_regex.apply(
                lambda x: x if x is None else x.group("first")
            )
            first_name.name = "first_name"
            list_columns_norm.append(first_name)
            # separate last name
            last_name = result_regex.apply(
                lambda x: x if x is None else x.group("last")
            )
            last_name.name = "last_name"
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
            age = date_birthday.apply(
                lambda x: int(relativedelta(datetime.now(), x).years)
            )
            # if the resulting age is zero, it is set to 1
            mask = age == 0
            age[mask] = 1
            mask = age < 0
            age[mask] = age[mask] + 100
            age.name = "age"
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
            list_columns_norm.append(data)
        elif column == "postal_code" or column == "location":
            if column == "postal_code":
                # find locacions
                codigo = df_university[column].apply(lambda x: int(x))
                locations = find_location_postalcode(column, codigo)
                list_columns_norm.append(locations)
                # the postal code are put as a string
                data = df_university[column].apply(lambda x: str(x))
                data.name = "postal_code"
                list_columns_norm.append(data)
            else:
                # the location lowercase str, no extra spaces, no hyphens
                data = (
                    df_university[column]
                    .str.lower()
                    .str.strip()
                    .str.replace("-", " ")
                    .str.replace("_", " ")
                )
                list_columns_norm.append(data)
                # find postal codes
                postal_codes = find_location_postalcode(column, data)
                data = postal_codes.apply(lambda x: str(x))
                data.name = "postal_code"
                list_columns_norm.append(data)

    # the final DataFrame is created
    list_result = []
    for i in range(len(list_columns_norm)):
        list_result.append(pd.DataFrame(list_columns_norm[i]))
    df_result = pd.concat(list_result, axis=1)
    return df_result


def find_location_postalcode(
    column: Optional[str], data: Optional[pd.DataFrame]
) -> pd.Series:
    """search for postal code or town

    Args:
        column (_type_): column name
        data (_type_): pd.Series according to column

    Returns:
        _type_: pd.Series with the requested data
    """
    # set path root
    root = Path.cwd()
    
    path_f = Path(root / "data" / "codigos_postales.csv")
    df_cod_loc = pd.read_csv(
        filepath_or_buffer=path_f, sep=",", index_col=None, encoding="utf-8"
    )

    if column == "postal_code":
        result = pd.merge(
            df_cod_loc,
            data,
            how="right",
            left_on="codigo_postal",
            right_on="postal_code",
        )
        result_f = (
            result.localidad.str.lower()
            .str.strip()
            .str.replace("-", " ")
            .str.replace("_", " ")
        )
        result_f.name = "location"
        return result_f
    else:
        df_cod_loc["local"] = df_cod_loc.localidad.str.lower()
        df_cod_loc.drop_duplicates(subset="localidad", keep="first", inplace=True)
        result = pd.merge(
            df_cod_loc, data, how="right", left_on="local", right_on="location"
        )
        result_f = result.codigo_postal
        result_f.name = "postal_code"
        return result_f
