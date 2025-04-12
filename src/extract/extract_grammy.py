import os
import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, Any


def extract_data() -> pd.DataFrame:
    """
    Extracts data from the Grammy dataset stored in a PostgreSQL database.

    This function reads the database credentials from a JSON file, connects to the
    PostgreSQL database using SQLAlchemy, and retrieves all rows from the
    'grammys_raw_data' table.

    It assumes the credentials file is located at 'Workshop_002/credentials.json'
    and that the script is executed from a path where moving one level up gives access
    to that folder.

    Returns:
        pd.DataFrame: A DataFrame containing the raw Grammy data from the database.

    Raises:
        FileNotFoundError: If the working directory is incorrect or the credentials file is missing.
        KeyError: If any expected keys are missing in the credentials JSON.
        SQLAlchemyError: If there's a problem connecting to or querying the database.
    """
    try:
        os.chdir(os.path.join("..", "..", "Workshop_002"))
    except FileNotFoundError:
        raise FileNotFoundError("The directory '../../Workshop_002' does not exist or is not accessible.")

    credentials_path = os.path.join("credentials.json")

    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credentials file not found at: {credentials_path}")

    with open(credentials_path, "r", encoding="utf-8") as file:
        credentials: Dict[str, Any] = json.load(file)

    try:
        db_host = credentials["db_host"]
        db_name = credentials["db_name"]
        db_user = credentials["db_user"]
        db_password = credentials["db_password"]
    except KeyError as e:
        raise KeyError(f"Missing key in credentials file: {e}")

    try:
        engine = create_engine(
            f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}"
        )
        query = "SELECT * FROM grammys_raw_data"
        with engine.connect() as conn:
            df = pd.read_sql(sql=query, con=conn.connection)
    except SQLAlchemyError as e:
        raise SQLAlchemyError(f"Database error: {e}")

    return df
