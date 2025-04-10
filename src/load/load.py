import pandas as pd
import json
import logging
from sqlalchemy import create_engine
from typing import Union
import os

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def create_connection_string() -> str:
    """
    Constructs the PostgreSQL connection string.

    Args:
        creds (dict): Dictionary with host, port, dbname, user, and password.

    Returns:
        str: PostgreSQL connection string.
    """
    try:
        os.chdir("../../Workshop_002")
    except FileNotFoundError:
        print("""
            FileNotFoundError - The directory may not exist or you are not located in the specified path.
            """)
    os.chdir("..")
    print(os.getcwd())

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
        if_exists (str): Behavior if the table exists ('fail', 'replace', 'append').

    Returns:
        None
    """
    conn_str = create_connection_string()
    engine = create_engine(conn_str)

    log.info(f"⬆️ Cargando DataFrame a la tabla '{table_name}' (modo: {if_exists})...")
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    log.info("✅ Carga completa.")

