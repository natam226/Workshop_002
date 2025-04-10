import os
import json
import pandas as pd
from sqlalchemy import create_engine
from typing import Dict, Any


def extract_data():

    """ Extract data of the grammy dataset from the database."""

    try:
        os.chdir("../../Workshop_002")
    except FileNotFoundError:
        print("""
            FileNotFoundError - The directory may not exist or you are not located in the specified path.
            """)
    os.chdir("..")
    print(os.getcwd())

    with open("Workshop_002/credentials.json", "r", encoding="utf-8") as file:
        credentials = json.load(file)

    db_host = credentials["db_host"]
    db_name = credentials["db_name"]
    db_user = credentials["db_user"]
    db_password = credentials["db_password"]
    
    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}")
    query = "SELECT * FROM grammys_raw_data"
    with engine.connect() as conn:
        df = pd.read_sql(sql=query, con=conn.connection)
    
    return df