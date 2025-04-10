# merge_datasets.py

import pandas as pd
import logging
import re
from typing import Union


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def expand_artists_column(df: pd.DataFrame, column: str = "artist") -> pd.DataFrame:
    """
    Expands rows with multiple artists in the specified column by splitting on common collaboration patterns.

    Args:
        df (pd.DataFrame): DataFrame containing artist information.
        column (str): Name of the column with artist names to be expanded.

    Returns:
        pd.DataFrame: DataFrame with one artist per row, normalized to lowercase and stripped of whitespace.
    """
    log.info(f"🔄 Expanding artists in column '{column}'...")

    # Common separators for collaborations
    separators = r';|,|&| Featuring | feat\.| Feat\.| ft\.|/| x '
    df = df.copy()
    df[column] = df[column].astype(str)

    df_expanded = df.assign(**{
        column: df[column].str.split(separators)
    }).explode(column)

    df_expanded[column] = df_expanded[column].str.strip().str.lower()
    return df_expanded


def normalize_artist_names(df: pd.DataFrame, column: str = "artist") -> pd.DataFrame:
    """
    Normalizes artist names by stripping and converting to lowercase.

    Args:
        df (pd.DataFrame): DataFrame to modify.
        column (str): Column name to normalize.

    Returns:
        pd.DataFrame: Modified DataFrame with normalized artist names.
    """
    log.info(f"🧼 Normalizing artist names in column '{column}'...")
    df[column] = df[column].astype(str).str.strip().str.lower()
    return df


def merge_datasets(
    df_spotify: pd.DataFrame,
    df_grammy: pd.DataFrame,
    df_api: pd.DataFrame
) -> pd.DataFrame:
    """
    Merges Spotify, Grammy, and Wikidata datasets by expanding and normalizing artist names.

    Args:
        df_spotify (pd.DataFrame): Preprocessed Spotify dataset.
        df_grammy (pd.DataFrame): Preprocessed Grammy dataset.
        df_wikidata (pd.DataFrame): Preprocessed Wikidata dataset.

    Returns:
        pd.DataFrame: Final merged DataFrame without duplicates or null values.
    """
    log.info("🚀 Starting merge of Spotify, Grammy, and Wikidata datasets...")


    df_spotify_exp = expand_artists_column(df_spotify, "artists").rename(columns={"artists": "artist"})
    df_grammy_exp = expand_artists_column(df_grammy, "artist")


    df_api = normalize_artist_names(df_api, "artist")


    log.info("🔗 Merging Spotify with Grammy data...")
    merged_spotify_grammy = pd.merge(
        df_spotify_exp,
        df_grammy_exp,
        on='artist',
        how='left',
        suffixes=('', '_grammy')
    )

    log.info("🔗 Merging result with Wikidata data...")
    final_merged = pd.merge(
        merged_spotify_grammy,
        df_api,
        on='artist',
        how='left',
        suffixes=('', '_wikidata')
    )

    final_merged = final_merged.drop_duplicates(subset=['track_id', 'artist'], keep='first')
    final_merged = final_merged.dropna().reset_index(drop=True)

    log.info(f"✅ Merge completed: {len(final_merged)} rows returned.")
    return final_merged
