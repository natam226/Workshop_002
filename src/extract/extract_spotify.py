import os
import pandas as pd


def extract_spotify_data() -> pd.DataFrame:
    """
    Extracts data from the Spotify dataset CSV file.

    The function constructs the path to the CSV file assuming it is located
    in a 'data' directory two levels above the current file's directory.
    It attempts to read the file into a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the data from 'spotify_dataset.csv'.

    Raises:
        FileNotFoundError: If the CSV file does not exist at the specified path.
        Exception: If there is an issue reading the CSV file.
    """
    base_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', '..', 'data')
    )
    csv_path = os.path.join(base_dir, 'spotify_dataset.csv')

    if not os.path.exists(csv_path):
        print(f"FileNotFoundError - The file '{csv_path}' was not found.\n"
              f"Current working directory: {os.getcwd()}")
        raise FileNotFoundError(f"File '{csv_path}' not found")

    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        raise

    return df
