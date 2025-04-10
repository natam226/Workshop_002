""" Transform grammys data for analysis. """

import pandas as pd
import logging
import re

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def delete_nominee_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Delete rows with null values in the 'nominee' column.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with null values removed.
    """
    logging.info("Deleting rows with null values in the 'nominee' column")
    return df.dropna(subset=['nominee'])


def delete_nulls_in_nonuseful(df: pd.DataFrame) -> pd.DataFrame:
    """Delete rows with null values in non-useful categories.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with null values removed.
    """
    logging.info("Deleting rows with null values in non-useful categories")
    categories_non_useful = [
    'Best Small Ensemble Performance (With or Without Conductor)',
    'Best Classical Vocal Performance',
    'Best Classical Vocal Soloist Performance',
    'Best Classical Performance - Instrumental Soloist or Soloists (With or Without Orchestra)',
    'Best Classical Performance - Vocal Soloist',
    'Best Performance - Instrumental Soloist or Soloists (With or Without Orchestra)',
    'Best Classical Performance - Vocal Soloist (With or Without Orchestra)'
    ]

    filter = (
        (df['artist'].isnull()) &
        (df['workers'].isnull()) &
        (df['category'].isin(categories_non_useful))
    )
    df = df[~filter]
    return df.reset_index(drop=True)


def impute_artist(df: pd.DataFrame) -> pd.DataFrame:
    """Impute missing values in the 'artist' column using the 'nominee' column.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with missing values imputed.
    """
    logging.info("Imputing missing values in the 'artist' column")
    condition = df['artist'].isnull() & df['workers'].isnull()
    df.loc[condition, 'artist'] = df.loc[condition, 'nominee']
    return df.reset_index(drop=True)

def imput_parenthesis_artists(df: pd.DataFrame) -> pd.DataFrame:
    """Impute missing values in the 'artist' column from the artist in the parenthesis in the 'workers' column.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with missing values imputed.
    """
    logging.info("Imputing missing values in the 'artist' column from the parenthesis")

    def extract_artist(workers):
        match = re.search(r'\((.*?)\)', workers)
        if match:
            return match.group(1)
        return None

    df["artist"] = (df.apply(lambda row:extract_artist(row["workers"])
            if pd.isna(row["artist"])
                else row["artist"], axis=1))
    return df.reset_index(drop=True)


def impute_artists_role(df: pd.DataFrame) -> pd.DataFrame:
    """Impute missing values in the 'artist' column using the roles that appear in the 'workers' column.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with missing values imputed.
    """
    logging.info("Imputing missing values in the 'artist' column using the 'role' column")

    def extract_artist(workers):
        if pd.isnull(workers):
            return None
        rule = re.match(r"([^,;]+), (soloist|composer|conductor|artist)", workers)
        if rule:
            return rule.group(1).strip()
        rule = re.match(r"(.+?(Featuring|&| and ).*?)(;|,|$)", workers, re.IGNORECASE)
        if rule:
            return rule.group(1).strip()
        return workers.strip()
    df['artist'] = df['artist'].fillna(df['workers'].apply(extract_artist))
    return df.reset_index(drop=True)


def replace_values(df: pd.DataFrame) -> pd.DataFrame:
    """Replace the value of '(Various Artists)' in the artist column for the value 'Various Artists' in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with specified values replaced.
    """
    logging.info("Replacing specific values in the DataFrame")
    df['artist'] = df['artist'].replace({'(Various Artists)': 'Various Artists'})
    return df.reset_index(drop=True)


def change_column_name(df: pd.DataFrame) -> pd.DataFrame:
    """Change the name of a column in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with renamed columns.
    """
    logging.info("Changing column names")
    df = df.rename(columns={'winner': 'nominated'})
    return df.reset_index(drop=True)


def delete_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Delete unused columns from the DataFrame.
    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with specified columns deleted.
    """
    logging.info("Deleting columns")
    df = df.drop(columns=['published_at', 'updated_at', 'img', 'workers'], axis=1)
    return df.reset_index(drop=True)


def transform_grammy_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the Grammy data for analysis.

    Args:
        df (pd.DataFrame): The DataFrame to transform.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    logging.info("Transforming Grammy data")

    df = delete_nominee_nulls(df)
    df = delete_nulls_in_nonuseful(df)
    df = impute_artist(df)
    df = imput_parenthesis_artists(df)
    df = impute_artists_role(df)
    df = replace_values(df)
    df = change_column_name(df)
    df = delete_columns(df)
    df['decade'] = (df['year'] // 10) * 10
    return df.reset_index(drop=True)