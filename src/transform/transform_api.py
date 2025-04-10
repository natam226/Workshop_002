import pandas as pd
from langdetect import detect
from tqdm import tqdm

# Palabras clave que indican que el premio no está en inglés
NO_ENGLISH_WORDS = [
    "stär um", "para", "prêmio", "premio", "prix", "voor", "de", "sus", "la", "das", "del", "der", "des",
    "el", "le", "pe", "stella", "sulla", "nagroda", "carriera", "réalta", "premi", "xelata",
    "tähti", "æresdoktor", "famen", "doktor", "oriel", "anfarwolion", "auf dem", "or merit", "kpakpando", "stäär üüb"
]

# Cache de idioma para evitar recalcular
award_lang_cache = {}

def is_english_filtered(text):
    text_l = str(text).lower().strip()
    if text not in award_lang_cache:
        try:
            award_lang_cache[text] = detect(text)
        except:
            award_lang_cache[text] = "unknown"
    if award_lang_cache[text] != "en":
        return False
    return not any(palabra in text_l for palabra in NO_ENGLISH_WORDS)

def valor_mas_comun(serie):
    if not serie.mode().empty:
        return serie.mode().iloc[0]
    elif not serie.dropna().empty:
        return serie.dropna().iloc[0]
    else:
        return "Unknown"

def transformation_api(df: pd.DataFrame) -> pd.DataFrame:
    # Paso 1: Filtrar premios válidos
    df_valid_awards = df[df['award'].notna()].copy()
    tqdm.pandas(desc="Filtrando premios válidos")
    df_valid_awards = df_valid_awards[df_valid_awards['award'].progress_apply(is_english_filtered)]

    # Paso 2: Agrupar premios válidos por artista
    premios_agrupados = df_valid_awards.groupby("artist")["award"].apply(lambda x: sorted(set(x))).reset_index()
    premios_agrupados["award_count"] = premios_agrupados["award"].apply(len)
    premios_agrupados["won_grammy"] = premios_agrupados["award"].apply(
        lambda premios: any("grammy" in str(premio).lower() for premio in premios)
    )
    premios_agrupados["awards_list"] = premios_agrupados["award"].apply(lambda x: "; ".join(x))

    # Paso 3: Agrupar atributos comunes por artista
    atributos_agrupados = df.groupby("artist").agg({
        "country": valor_mas_comun,
        "gender": valor_mas_comun,
        "album_count": valor_mas_comun
    }).reset_index()

    # Paso 4: Unir premios con atributos
    df_final = atributos_agrupados.merge(
        premios_agrupados.drop(columns=["award"]),
        on="artist",
        how="left"
    )

    # Paso 5: Completar valores faltantes para artistas sin premios
    df_final["award_count"] = df_final["award_count"].fillna(0).astype(int)
    df_final["won_grammy"] = df_final["won_grammy"].fillna(False)
    df_final["awards_list"] = df_final["awards_list"].fillna("No awards")

    # Paso 6: Reordenar columnas
    df_final = df_final[[
        "artist", "gender", "country", "award_count",
        "won_grammy", "awards_list", "album_count"
    ]]

    return df_final
