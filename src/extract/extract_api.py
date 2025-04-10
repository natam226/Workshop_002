import os
import time
import logging
import pandas as pd
import requests
from tqdm import tqdm


WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {
    "Accept": "application/sparql-results+json",
    "User-Agent": "Workshop/1.0 (ejemplo@gmail.com)"
}

ARTISTS_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'artists.csv'))
MAX_QUERY_SIZE = 60000

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def extract_api() -> pd.DataFrame:
    artistas_unicos = _cargar_y_limpiar_artistas(ARTISTS_CSV)
    resultados = _consultar_wikidata(artistas_unicos)
    columnas_ordenadas = ["artist", "country", "award", "gender", "album_count"]
    return pd.DataFrame(resultados, columns=columnas_ordenadas)


def limpiar_nombre(nombre: str) -> str:
    if pd.isna(nombre) or not nombre.strip():
        return None
    nombre = nombre.replace("\\", "").replace('"', '').replace("'", "")
    nombre = nombre.replace("/", " ").replace("&", "and")
    return nombre.strip()


def _cargar_y_limpiar_artistas(ruta_csv: str) -> list:
    df = pd.read_csv(ruta_csv, header=None, names=["raw"])
    nombres_limpios = [limpiar_nombre(nombre) for nombre in df["raw"]]
    artistas_unicos = sorted(set([nombre for nombre in nombres_limpios if nombre]))
    logging.info(f"✅ Total artistas únicos: {len(artistas_unicos)}")
    return artistas_unicos


def construir_query_sparql(artistas: list) -> str:
    values = "\n".join([f'"{nombre}"@en' for nombre in artistas])
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


def _obtener_datos_wikidata(artistas_batch):
    query = construir_query_sparql(artistas_batch)
    try:
        response = requests.post(WIKIDATA_ENDPOINT, data={"query": query}, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error en SPARQL: {e}")
        return None


def _consultar_wikidata(artistas_unicos: list) -> list:
    resultados = []
    i = 0

    logging.info("🚀 Consultando Wikidata...")
    with tqdm(total=len(artistas_unicos), desc="🔎 Batches SPARQL") as pbar:
        while i < len(artistas_unicos):
            batch_size = 80
            batch_success = False

            while batch_size > 0 and not batch_success:
                batch = artistas_unicos[i:i+batch_size]
                query = construir_query_sparql(batch)

                if len(query.encode("utf-8")) > MAX_QUERY_SIZE:
                    batch_size -= 5
                    continue

                data = _obtener_datos_wikidata(batch)
                if data:
                    for row in data["results"]["bindings"]:
                        resultados.append({
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
                    batch_size = batch_size // 2

            if not batch_success:
                logging.warning(f"⚠️  Saltando artista en índice {i}: {artistas_unicos[i]}")
                i += 1
                pbar.update(1)

    return resultados
