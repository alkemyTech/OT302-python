import pandas as pd
from functions.logging_config import CustomLogger

# Create 2 loggers
logger_kennedy = CustomLogger(logger_name="kennedy", file_name="universidad j. f. kennedy")
logger_latinoamericana = CustomLogger(
    logger_name="latinoamericana", file_name="facultad latinoamericana de ciencias sociales"
)


def quit_spaces_dashes(string):
    """Function used to replace '-' for spaces, to strip and to lower the letters"""

    string = string.replace("-", " ").strip().lower()
    return string


def clean_name(name):
    """Function that replaces the suffixes and postfixes for spaces, to strip and to lower the letters"""

    # Suffixes or postfixes to delete
    to_delete = ["mr.", "mrs.", "ms.", "miss", "dr.", "md", "dds", "ii", "dvm", "jr.", "phd"]

    # Replace "-" for a space
    name = name.replace("-", " ").lower()

    # Delete suffixes and postfixes
    for substring in to_delete:
        if substring in name:
            name = name.replace(substring, "").strip()
    return name


def gender(g):
    """Function used to display the correct format of gender"""

    g = g.lower()
    if g == "m":
        return "male"
    if g == "f":
        return "female"


def common_tasks(df, path_to_data_docker):
    """Function used to perform tasks that are common to the 2 DataFrames"""

    # Load the location/postal_code csv https://drive.google.com/uc?id=1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ
    try:
        df_loc_pc = pd.read_csv(f"{path_to_data_docker}codigos_postales.csv")
    except:
        logger_kennedy.error("Error: There was a problem to load the csv of location/postal_code")
        logger_latinoamericana.error("Error: There was a problem to load the csv of location/postal_code")

    # Edit column "localidad" in the location/postal_code csv to have lower strings without spaces #
    df_loc_pc["localidad"] = df_loc_pc["localidad"].apply(quit_spaces_dashes)

    # Edit "universities" to quit dashes and spaces
    df["university"] = df["university"].apply(quit_spaces_dashes)

    # Edit "careers" to quit dashes and spaces
    df["career"] = df["career"].apply(quit_spaces_dashes)

    # Clean column "name" from suffixes, postfixes, dashes and spaces
    df["name"] = df["name"].apply(clean_name)

    # Create "first_name" column and take the first_name from "name" column
    df["first_name"] = df["name"].apply(lambda x: x.split()[0])

    # Create "last_name" column and take the last_name from "name" column
    df["last_name"] = df["name"].apply(lambda x: x.split()[1])

    # Drop the column "name"
    df.drop(columns=["name"], inplace=True)

    # Edit the column "gender" to have as options male/female
    df["gender"] = df["gender"].apply(gender)

    # Edit the column "age". From the query if the age value was below zero means that 100 years must be added to the value
    df["age"] = df["age"].apply(lambda x: int(x + 100) if x < 0 else int(x))

    # Edit the column "email" to lower
    df["email"] = df["email"].apply(lambda x: x.lower().strip())

    return (df, df_loc_pc)


def reorder_columns(df):
    """Function used to reorder the columns in the desired order"""
    df = df[
        [
            "university",
            "career",
            "inscription_date",
            "first_name",
            "last_name",
            "gender",
            "age",
            "postal_code",
            "location",
            "email",
        ]
    ]
    return df


def transform_univ_kennedy(path_to_data_docker, univ_kennedy_file_name):
    """Function used to transform the data from universidad j. f. kennedy from csv to txt"""

    logger_kennedy.info("Starting to transform the data")

    # Load the kennedy csv from the docker folder
    try:
        df = pd.read_csv(f"{path_to_data_docker}{univ_kennedy_file_name}.csv")
    except:
        logger_kennedy.error("Error: There was a problem to load the csv of universidad j. f. kennedy")

    # Perform common tasks to the 2 DataFrames
    df, df_loc_pc = common_tasks(df, path_to_data_docker)

    # Merge the 2 DataFrames on postal_code columns
    df = df.merge(df_loc_pc, left_on="postal_code", right_on="codigo_postal")

    # Transform "postal_code" to str
    df["postal_code"] = df["postal_code"].apply(lambda x: str(x))

    # Drop the column "codigo_postal"
    df.drop(columns=["codigo_postal"], inplace=True)

    # Rename column "localidades" to "location"
    df.rename(columns={"localidad": "location"}, inplace=True)

    # Reorder columns
    df = reorder_columns(df)

    # Save the results to txt
    try:
        df.to_csv(f"{path_to_data_docker}{univ_kennedy_file_name}.txt", index=False, sep=",")
        logger_kennedy.info("The txt of universidad j. f. kennedy is saved")
    except:
        logger_kennedy.error("Error: There was a problem to save the txt of universidad j. f. kennedy")


def transform_facu_latinoamericana(path_to_data_docker, facu_latinoamericana_file_name):
    """Function used to transform the data from facultad latinoamericana de ciencias sociales from csv to txt"""

    logger_latinoamericana.info("Starting to transform the data")

    # Load the latinoamericana csv from the docker folder
    try:
        df = pd.read_csv(f"{path_to_data_docker}{facu_latinoamericana_file_name}.csv")
    except:
        logger_latinoamericana.error(
            "Error: There was a problem to load the csv of facultad latinoamericana de ciencias sociales"
        )

    # Perform common tasks to the 2 DataFrames
    df, df_loc_pc = common_tasks(df, path_to_data_docker)

    # Edit the column "location" to quit dashes and extra spaces
    df["location"] = df["location"].apply(quit_spaces_dashes)

    # Drop duplicate postal_code for the same location
    df_loc_pc.drop_duplicates("localidad", inplace=True)

    # Merge the 2 DataFrames on location columns
    df = df.merge(df_loc_pc, left_on="location", right_on="localidad")

    # Drop column "localidad"
    df.drop(columns=["localidad"], inplace=True)

    # Rename column "codigo_postal" to "postal_code"
    df.rename(columns={"codigo_postal": "postal_code"}, inplace=True)

    # Transform "postal_code" to str
    df["postal_code"] = df["postal_code"].apply(lambda x: str(x))

    # Reorder columns
    df = reorder_columns(df)

    # Save the results to txt
    try:
        df.to_csv(f"{path_to_data_docker}{facu_latinoamericana_file_name}.txt", index=False, sep=",")
        logger_latinoamericana.info("The txt of facultad latinoamericana de ciencias sociales is saved")
    except:
        logger_latinoamericana.error(
            "Error: There was a problem to save the txt of facultad latinoamericana de ciencias sociales"
        )


def pandas_processing(path_to_data_docker, univ_kennedy_file_name, facu_latinoamericana_file_name):
    transform_univ_kennedy(path_to_data_docker, univ_kennedy_file_name)
    transform_facu_latinoamericana(path_to_data_docker, facu_latinoamericana_file_name)
