import pandas as pd
import json
import logging
from sqlalchemy import create_engine
from typing import Union

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def load_db_credentials(json_path: str) -> Dict[str, str]:
 
    """
    Loads database credentials from JSON file
    
    Args:
        json_path: Path to JSON credentials file
        
    Returns:
        Dictionary with connection parameters
        
    Raises:
        ValueError: If JSON file is invalid or missing required keys
    """
    try:
        with open(json_path, 'r') as f:
            creds = json.load(f)
            
        required_keys = ['db_host', 'db_name', 'db_user', 'db_password']
        if not all(key in creds for key in required_keys):
            missing = set(required_keys) - set(creds.keys())
            raise ValueError(f"Missing required keys in credentials: {missing}")
            
        return creds
        
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format in credentials file")
    except FileNotFoundError:
        raise ValueError(f"Credentials file not found at {json_path}")


def create_connection_string(creds: dict) -> str:
    """
    Constructs the PostgreSQL connection string.

    Args:
        creds (dict): Dictionary with host, port, dbname, user, and password.

    Returns:
        str: PostgreSQL connection string.
    """
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
    creds = load_credentials()
    conn_str = create_connection_string(creds)
    engine = create_engine(conn_str)

    log.info(f"⬆️ Cargando DataFrame a la tabla '{table_name}' (modo: {if_exists})...")
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    log.info("✅ Carga completa.")


CREDS_FILE = "/home/ubuntu/Escritorio/Leukemia-Cancer-Risk-ETL/credentialsdb.json"