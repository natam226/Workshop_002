import pandas as pd
from langdetect import detect
from tqdm import tqdm

NO_ENGLISH_WORDS = [
    "stär um", "para", "prêmio", "premio", "prix", "voor", "de", "sus", "la", "das", "del", "der", "des",
    "el", "le", "pe", "stella", "sulla", "nagroda", "carriera", "réalta", "premi", "xelata",
    "tähti", "æresdoktor", "famen", "doktor", "oriel", "anfarwolion", "auf dem", "or merit",
    "kpakpando", "stäär üüb"
]

award_lang_cache = {}

def is_english_filtered(text):
    """
    Detects if the input text is in English and does not contain keywords from other languages.

    Args:
        text (str): Award name or description.

    Returns:
        bool: True if the award is considered English, False otherwise.
    """
    text_l = str(text).lower().strip()
    if text not in award_lang_cache:
        try:
            award_lang_cache[text] = detect(text)
        except Exception:
            award_lang_cache[text] = "unknown"

    if award_lang_cache[text] != "en":
        return False

    return not any(word in text_l for word in NO_ENGLISH_WORDS)

def most_common_value(series):
    """
    Returns the most common (mode) value of a pandas Series, or the first non-null value if no mode exists.

    Args:
        series (pd.Series): Input series.

    Returns:
        Any: Most common or first valid value.
    """
    if not series.mode().empty:
        return series.mode().iloc[0]
    elif not series.dropna().empty:
        return series.dropna().iloc[0]
    else:
        return "Unknown"

def transformation_api(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms an artist dataset by cleaning and aggregating awards and attributes per artist.

    Args:
        df (pd.DataFrame): Raw input data including awards and artist metadata.

    Returns:
        pd.DataFrame: Cleaned and aggregated dataset by artist.
    """
    df_valid_awards = df[df['award'].notna()].copy()
    tqdm.pandas(desc="Filtering valid awards")
    df_valid_awards = df_valid_awards[df_valid_awards['award'].progress_apply(is_english_filtered)]

    grouped_awards = df_valid_awards.groupby("artist")["award"].apply(lambda x: sorted(set(x))).reset_index()
    grouped_awards["award_count"] = grouped_awards["award"].apply(len)
    grouped_awards["won_grammy"] = grouped_awards["award"].apply(
        lambda awards: any("grammy" in str(award).lower() for award in awards)
    )
    grouped_awards["awards_list"] = grouped_awards["award"].apply(lambda x: "; ".join(x))

    grouped_attributes = df.groupby("artist").agg({
        "country": most_common_value,
        "gender": most_common_value,
        "album_count": most_common_value
    }).reset_index()

    df_final = grouped_attributes.merge(
        grouped_awards.drop(columns=["award"]),
        on="artist",
        how="left"
    )

    df_final["award_count"] = df_final["award_count"].fillna(0).astype(int)
    df_final["won_grammy"] = df_final["won_grammy"].fillna(False)
    df_final["awards_list"] = df_final["awards_list"].fillna("No awards")

    df_final = df_final[[
        "artist", "gender", "country", "award_count",
        "won_grammy", "awards_list", "album_count"
    ]]

    return df_final
