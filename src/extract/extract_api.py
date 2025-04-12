import os
import time
import logging
import pandas as pd
import requests
from tqdm import tqdm

# Constants
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {
    "Accept": "application/sparql-results+json",
    "User-Agent": "Workshop/1.0 (ejemplo@gmail.com)"
}
ARTISTS_CSV = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'artists.csv')
)
MAX_QUERY_SIZE = 60000

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def extract_api() -> pd.DataFrame:
    """
    Extracts artist data from Wikidata using SPARQL queries and 
    returns the results as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing columns 
                      ['artist', 'country', 'award', 'gender', 'album_count'].
    """
    unique_artists = _load_and_clean_artists(ARTISTS_CSV)
    results = _query_wikidata(unique_artists)
    ordered_columns = ["artist", "country", "award", "gender", "album_count"]
    return pd.DataFrame(results, columns=ordered_columns)


def clean_name(name: str) -> str:
    """
    Cleans and normalizes an artist's name.

    Args:
        name (str): The original artist name.

    Returns:
        str: A cleaned and formatted name, or None if invalid.
    """
    if pd.isna(name) or not name.strip():
        return None
    name = name.replace("\\", "").replace('"', "").replace("'", "")
    name = name.replace("/", " ").replace("&", "and")
    return name.strip()


def _load_and_clean_artists(csv_path: str) -> list:
    """
    Loads a CSV file with artist names and returns a sorted list 
    of unique, cleaned names.

    Args:
        csv_path (str): Absolute path to the artist CSV file.

    Returns:
        list: Sorted list of unique artist names.
    """
    df = pd.read_csv(csv_path, header=None, names=["raw"])
    cleaned_names = [clean_name(name) for name in df["raw"]]
    unique_artists = sorted(set(name for name in cleaned_names if name))
    logging.info(f"✅ Total unique artists: {len(unique_artists)}")
    return unique_artists


def build_sparql_query(artists: list) -> str:
    """
    Builds a SPARQL query to fetch data for a batch of artists.

    Args:
        artists (list): List of artist names.

    Returns:
        str: A formatted SPARQL query string.
    """
    values = "\n".join([f'"{name}"@en' for name in artists])
    return f"""
    SELECT ?artistLabel ?countryLabel ?awardLabel ?genderLabel (COUNT(?album) AS ?album_count) WHERE {{
      VALUES ?name {{ {values} }}
      ?artist rdfs:label ?name.
      OPTIONAL {{ ?artist wdt:P166 ?award. }}
      OPTIONAL {{ ?artist wdt:P27 ?country. }}
      OPTIONAL {{ ?artist wdt:P21 ?gender. }}
      OPTIONAL {{
        ?album wdt:P31 wd:Q482994.
        ?album wdt:P175 ?artist.
      }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    GROUP BY ?artistLabel ?countryLabel ?awardLabel ?genderLabel
    """


def _get_wikidata_results(artist_batch: list) -> dict | None:
    """
    Sends a POST request to Wikidata with a SPARQL query 
    for a batch of artists.

    Args:
        artist_batch (list): A list of artist names for the query.

    Returns:
        dict or None: JSON response from Wikidata if successful, else None.
    """
    query = build_sparql_query(artist_batch)
    try:
        response = requests.post(
            WIKIDATA_ENDPOINT, data={"query": query}, headers=HEADERS
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ SPARQL request error: {e}")
        return None


def _query_wikidata(unique_artists: list) -> list:
    """
    Queries Wikidata in batches for the given list of unique artists.

    Args:
        unique_artists (list): List of cleaned, unique artist names.

    Returns:
        list: A list of dictionaries with artist data from Wikidata.
    """
    results = []
    i = 0

    logging.info("🚀 Querying Wikidata...")
    with tqdm(total=len(unique_artists), desc="🔎 SPARQL Batches") as pbar:
        while i < len(unique_artists):
            batch_size = 80
            batch_success = False

            while batch_size > 0 and not batch_success:
                batch = unique_artists[i:i + batch_size]
                query = build_sparql_query(batch)

                if len(query.encode("utf-8")) > MAX_QUERY_SIZE:
                    batch_size -= 5
                    continue

                data = _get_wikidata_results(batch)
                if data:
                    for row in data["results"]["bindings"]:
                        results.append({
                            "artist": row.get("artistLabel", {}).get("value", ""),
                            "country": row.get("countryLabel", {}).get("value", ""),
                            "award": row.get("awardLabel", {}).get("value", "No awards"),
                            "gender": row.get("genderLabel", {}).get("value", "Unknown"),
                            "album_count": row.get("album_count", {}).get("value", "0")
                        })
                    batch_success = True
                    i += batch_size
                    pbar.update(batch_size)
                    time.sleep(0.8)
                else:
                    batch_size //= 2

            if not batch_success:
                logging.warning(f"⚠️ Skipping artist at index {i}: {unique_artists[i]}")
                i += 1
                pbar.update(1)

    return results
