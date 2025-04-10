import os
import pandas as pd
from typing import Any


def extract_spotify_data() -> pd.DataFrame:
    """
    Extract data from the Spotify dataset CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the data from spotify_dataset.csv
    """
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data'))

    csv_path = os.path.join(BASE_DIR, 'spotify_dataset.csv')
    if not os.path.exists(csv_path):
        print(f"""
            FileNotFoundError - The file {csv_path} was not found in the current directory: {os.getcwd()}
            """)
        raise FileNotFoundError(f"File {csv_path} not found")

    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
    except Exception as e:
        print(f"""
            Error reading the CSV file: {str(e)}
            """)
        raise

    return df