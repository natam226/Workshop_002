import os
import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from typing import Union


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def create_connection_string() -> str:
    """
    Constructs the PostgreSQL connection string from a JSON credentials file.

    The function attempts to change directory to reach the location of the 
    credentials file. Then, it reads the credentials and builds the 
    SQLAlchemy-compatible connection string.

    Returns:
        str: PostgreSQL connection string.

    Raises:
        FileNotFoundError: If the directory or credentials file is not found.
        KeyError: If any required key is missing from the credentials.
    """
    try:
        os.chdir("../../Workshop_002")
    except FileNotFoundError:
        print("FileNotFoundError - Directory does not exist or wrong working path.")

    os.chdir("..")
    print(f"Current working directory: {os.getcwd()}")

    with open("Workshop_002/credentialsdb.json", "r", encoding="utf-8") as file:
        credentials = json.load(file)

    db_host = credentials["db_host"]
    db_name = credentials["db_name"]
    db_user = credentials["db_user"]
    db_password = credentials["db_password"]

    return f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}"


def load_to_postgresql(df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> None:
    """
    Loads a DataFrame into a PostgreSQL table.

    Args:
        df (pd.DataFrame): DataFrame to upload.
        table_name (str): Name of the destination table.
        if_exists (str): Behavior if the table exists:
                         'fail', 'replace', or 'append'.

    Returns:
        None
    """
    conn_str = create_connection_string()
    engine = create_engine(conn_str)

    log.info(f"Loading DataFrame into table '{table_name}' (mode: {if_exists})...")
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    log.info("DataFrame successfully loaded into the database.")
