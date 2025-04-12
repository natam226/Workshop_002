""" Transform Grammy data for analysis. """

import pandas as pd
import logging
import re

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def drop_null_nominees(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with null values in the 'nominee' column.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    logging.info("Dropping rows with null values in the 'nominee' column")
    return df.dropna(subset=['nominee'])


def drop_nulls_in_nonessential_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with null values in non-essential award categories.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    logging.info("Dropping rows with null values in non-essential categories")
    non_essential_categories = [
        'Best Small Ensemble Performance (With or Without Conductor)',
        'Best Classical Vocal Performance',
        'Best Classical Vocal Soloist Performance',
        'Best Classical Performance - Instrumental Soloist or Soloists (With or Without Orchestra)',
        'Best Classical Performance - Vocal Soloist',
        'Best Performance - Instrumental Soloist or Soloists (With or Without Orchestra)',
        'Best Classical Performance - Vocal Soloist (With or Without Orchestra)'
    ]

    mask = (
        df['artist'].isnull() &
        df['workers'].isnull() &
        df['category'].isin(non_essential_categories)
    )
    return df[~mask].reset_index(drop=True)


def impute_artist_from_nominee(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing 'artist' values using the 'nominee' column when appropriate.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: Updated DataFrame.
    """
    logging.info("Imputing missing 'artist' values using 'nominee'")
    condition = df['artist'].isnull() & df['workers'].isnull()
    df.loc[condition, 'artist'] = df.loc[condition, 'nominee']
    return df.reset_index(drop=True)


def impute_artist_from_parenthesis(df: pd.DataFrame) -> pd.DataFrame:
    """Extract artist names from parentheses in the 'workers' column.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: Updated DataFrame.
    """
    logging.info("Imputing 'artist' values from parentheses in 'workers'")

    def extract_from_parentheses(workers):
        match = re.search(r'\((.*?)\)', str(workers))
        return match.group(1) if match else None

    df["artist"] = df.apply(
        lambda row: extract_from_parentheses(row["workers"]) if pd.isna(row["artist"]) else row["artist"],
        axis=1
    )
    return df.reset_index(drop=True)


def impute_artist_from_roles(df: pd.DataFrame) -> pd.DataFrame:
    """Impute missing 'artist' values by extracting them from roles in the 'workers' column.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: Updated DataFrame.
    """
    logging.info("Imputing missing 'artist' values using roles in 'workers'")

    def extract_artist(workers):
        if pd.isnull(workers):
            return None
        match = re.match(r"([^,;]+), (soloist|composer|conductor|artist)", workers)
        if match:
            return match.group(1).strip()
        match = re.match(r"(.+?(Featuring|&| and ).*?)(;|,|$)", workers, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return workers.strip()

    df['artist'] = df['artist'].fillna(df['workers'].apply(extract_artist))
    return df.reset_index(drop=True)


def replace_artist_values(df: pd.DataFrame) -> pd.DataFrame:
    """Replace specific values in the 'artist' column.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: Updated DataFrame.
    """
    logging.info("Replacing specific artist values")
    df['artist'] = df['artist'].replace({'(Various Artists)': 'Various Artists'})
    return df.reset_index(drop=True)


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to standardize naming.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: Updated DataFrame.
    """
    logging.info("Renaming columns")
    return df.rename(columns={'winner': 'nominated'}).reset_index(drop=True)


def drop_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop unnecessary columns from the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    logging.info("Dropping unused columns")
    return df.drop(columns=['published_at', 'updated_at', 'img', 'workers'], axis=1).reset_index(drop=True)


def transform_grammy_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all transformation steps to prepare Grammy data for analysis.

    Args:
        df (pd.DataFrame): Raw Grammy data.

    Returns:
        pd.DataFrame: Cleaned and transformed data.
    """
    logging.info("Starting transformation of Grammy data")

    df = drop_null_nominees(df)
    df = drop_nulls_in_nonessential_categories(df)
    df = impute_artist_from_nominee(df)
    df = impute_artist_from_parenthesis(df)
    df = impute_artist_from_roles(df)
    df = replace_artist_values(df)
    df = rename_columns(df)
    df = drop_unused_columns(df)
    df['decade'] = (df['year'] // 10) * 10

    return df.reset_index(drop=True)
