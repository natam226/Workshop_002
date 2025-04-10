import os
import pandas as pd
from typing import Any


def extract_spotify_data() -> pd.DataFrame:
    """
    Extract data from the Spotify dataset CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the data from spotify_dataset.csv
    """
    try:
        os.chdir("../../Workshop_002")
    except FileNotFoundError:
        print("""
            FileNotFoundError - The directory may not exist or you are not located in the specified path.
            """)
        raise 

    csv_path = "Workshop_002/data/spotify_dataset.csv"
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