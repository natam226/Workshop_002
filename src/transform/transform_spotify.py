""" Transform Spotify data for analysis. """

import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def delete_unnecessary_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Delete unnecessary columns from the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with specified columns deleted.
    """
    logging.info("Deleting unnecessary columns from the DataFrame.")
    return df.drop(columns=['Unnamed:0'], errors='ignore')


def drop_null_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows with null values from the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with null values dropped.
    """
    logging.info("Dropping rows with null values from the DataFrame.")
    return df.dropna().reset_index(drop=True)


def drop_duplicated_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop duplicated rows from the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with duplicated rows dropped.
    """
    logging.info("Dropping duplicated rows from the DataFrame.")
    return df.drop_duplicates().reset_index(drop=True)


def drop_duplicates_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop duplicated rows based on the 'id' column from the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with duplicated rows based on 'id' dropped.
    """
    logging.info("Dropping duplicated rows based on the 'id' column from the DataFrame.")
    return df.drop_duplicates(subset=['track_id']).reset_index(drop=True)


def mapping_genre(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map genre names into broader categories to a better use of the data.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with mapped genre names.
    """
    logging.info("Mapping genre names in the DataFrame.")
    genre_mapping = {
      'Rock': ['alt-rock', 'alternative', 'grunge', 'hard-rock', 'psych-rock', 'rock', 'rock-n-roll','rockabilly', 'indie', 'garage', 'j-rock'],
      'Metal': ['black-metal', 'death-metal', 'heavy-metal', 'metal', 'metalcore', 'grindcore','industrial'],
      'Punk': ['punk', 'punk-rock', 'emo'],
      'Pop': ['pop', 'power-pop', 'synth-pop', 'k-pop', 'j-pop', 'cantopop', 'mandopop','indie-pop', 'british', 'swedish'],
      'Film/Show Music': ['pop-film', 'disney', 'show-tunes', 'anime'],
      'Electronic': ['electronic', 'electro', 'idm', 'trip-hop'],
      'Dance': ['dance', 'club', 'edm'],
      'House': ['house', 'deep-house', 'chicago-house', 'progressive-house', 'detroit-techno','j-dance'],
      'Techno': ['techno', 'minimal-techno'],
      'Bass Music': ['dubstep', 'drum-and-bass', 'dub', 'breakbeat', 'hardstyle'],
      'Hip-Hop': ['hip-hop', 'r-n-b'],
      'Reggae/Dancehall': ['reggae', 'dancehall', 'reggaeton'],
      'Jazz': ['jazz', 'groove'],
      'Blues': ['blues', 'bluegrass', 'honky-tonk'],
      'Soul/Funk': ['soul', 'funk', 'gospel'],
      'Country': ['country'],
      'Folk': ['folk', 'singer-songwriter'],
      'Latin': ['latin', 'latino', 'salsa', 'samba', 'pagode', 'sertanejo', 'brazil', 'mpb','tango', 'spanish', 'forro'],
      'World Music': ['afrobeat', 'indian', 'iranian', 'malay', 'turkish', 'french', 'german','world-music'],
      'Classical': ['classical', 'opera', 'piano'],
      'Instrumental': ['acoustic', 'guitar', 'new-age'],
      'Ambient/Chill': ['ambient', 'chill', 'sleep', 'study'],
      'Mood': ['happy', 'sad', 'romance'],
      'Children': ['children', 'kids'],'Comedy/Novelty': ['comedy'],'Disco': ['disco'],'Goth': ['goth'],'Ska': ['ska'],'Party': ['party'],'J-Idol': ['j-idol']
    }
    genre_category_mapping = {genre: category for category, genres in genre_mapping.items() for genre in genres}
    df["track_genre"] = df["track_genre"].map(genre_category_mapping)
    return df.reset_index(drop=True)


def drop_duplicates_by_content(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop duplicated rows ignoring the "track_name" and "artist" columns of the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with duplicated rows based on content dropped.
    """
    subset_cols = [col for col in df.columns if col not in ["track_id", "album_name"]]
    logging.info("Dropping duplicated rows based on the content of the DataFrame.")
    return df.drop_duplicates(subset=subset_cols).reset_index(drop=True)


def keep_more_popular(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep the most popular track for each artist in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with the most popular track for each artist.
    """
    logging.info("Keeping the most popular track for each artist in the DataFrame.")
    idx = df.groupby(['track_name', 'artists'])['popularity'].idxmax()
    return df.loc[idx].reset_index(drop=True)


def change_duration(df: pd.DataFrame) -> pd.DataFrame:
    """
    Change the duration of tracks in the DataFrame from milliseconds to minutes.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with duration in minutes.
    """
    logging.info("Changing the duration of tracks from milliseconds to minutes.")
    df['duration_min'] = df['duration_ms'] / 60000
    df.drop(columns=['duration_ms'], inplace=True, errors='ignore')
    return df.reset_index(drop=True)


def categorize_popularity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorize the popularity of tracks in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with categorized popularity.
    """
    logging.info("Categorizing the popularity of tracks in the DataFrame.")
    bins = [0, 30, 60, 80, 100]
    labels = ['Low', 'Medium', 'High', 'Very High']
    df['popularity'] = pd.cut(df['popularity'], bins=bins, labels=labels)
    return df.reset_index(drop=True)


def categorize_danceability(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorize the danceability of tracks in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with categorized danceability.
    """
    logging.info("Categorizing the danceability of tracks in the DataFrame.")
    bins = [0, 0.3, 0.6, 1]
    labels = ['Low', 'Medium', 'High']
    df['danceability'] = pd.cut(df['danceability'], bins=bins, labels=labels)
    return df.reset_index(drop=True)

def categorize_energy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorize the energy of tracks in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with categorized energy.
    """
    logging.info("Categorizing the energy of tracks in the DataFrame.")
    bins=[0, 0.3, 0.7, 1]
    labels=['Low', 'Medium', 'High']
    df['energy'] = pd.cut(df['energy'], bins=bins, labels=labels)
    return df.reset_index(drop=True)


def categorize_duration(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorize the duration of tracks in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with categorized duration.
    """
    logging.info("Categorizing the duration of tracks in the DataFrame.")
    bins=[0, 2, 3.5, 5, 10, 20]
    labels=['Very Short', 'Short', 'Average', 'Long', 'Very Long']
    df['duration_min'] = pd.cut(df['duration_min'], bins=bins, labels=labels)
    return df.reset_index(drop=True)


def categorize_valence(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorize the valence of tracks in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with categorized valence.
    """
    logging.info("Categorizing the valence of tracks in the DataFrame.")
    bins=[0, 0.2, 0.4, 0.6, 0.8, 1]
    labels=['Very Sad','Sad','Neutral','Happy','Very Happy']
    df['valence'] = pd.cut(df['valence'], bins=bins, labels=labels)
    return df.reset_index(drop=True)


def create_boolean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create boolean columns for the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with boolean columns.
    """
    logging.info("Creating boolean columns in the DataFrame.")
    df['is_loud'] = df['loudness'] > -5
    df['is_live'] = df['liveness'] > 0.8
    return df.reset_index(drop=True)


def delete_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Delete specified columns from the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with specified columns deleted.
    """
    logging.info("Deleting specified columns from the DataFrame.")
    return df.drop(columns=['loudness', 'liveness','key', 'mode', 'time_signature', 'tempo', "speechiness", "acousticness", "instrumentalness"], errors='ignore')


def transform_spotify_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the Spotify data for analysis.

    Args:
        df (pd.DataFrame): The DataFrame to transform.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    logging.info("Starting transformation of Spotify data.")
    df = delete_unnecessary_columns(df)
    df = drop_null_values(df)
    df = drop_duplicated_values(df)
    df = drop_duplicates_id(df)
    df = mapping_genre(df)
    df = drop_duplicates_by_content(df)
    df = keep_more_popular(df)
    df = change_duration(df)
    df = categorize_popularity(df)
    df = categorize_danceability(df)
    df = categorize_energy(df)
    df = categorize_duration(df)
    df = categorize_valence(df)
    df = create_boolean(df)
    df = delete_columns(df)

    logging.info("Transformation of Spotify data completed.")
    return df.reset_index(drop=True)