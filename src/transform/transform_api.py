import pandas as pd
from langdetect import detect
from tqdm import tqdm

# Palabras clave que sugieren idioma diferente al inglés
NO_ENGLISH_WORDS = [
    "stär um", "para", "prêmio", "premio", "prix", "voor", "de", "sus", "la", "das", "del", "der", "des",
    "el", "le", "pe", "stella", "sulla", "nagroda", "carriera", "réalta", "premi", "xelata",
    "tähti", "æresdoktor", "famen", "doktor", "oriel", "anfarwolion", "auf dem", "or merit", "kpakpando", "stäär üüb"
]

# Cache para el idioma detectado
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

    for palabra in PALABRAS_NO_INGLESES:
        if palabra in text_l:
            return False

    return True

def most_common_value(serie):
    if not serie.mode().empty:
        return serie.mode().iloc[0]
    elif not serie.dropna().empty:
        return serie.dropna().iloc[0]
    else:
        return None

def transformation_api(df: pd.DataFrame) -> pd.DataFrame:
    # Limpieza inicial
    df['death'] = df['death'].notna()
    df['death'] = df['death'].map({False: 'alive', True: 'deceased'})
    df['country'] = df['country'].fillna('Unknown')
    df = df.drop_duplicates()

    # Filtrar premios en inglés
    tqdm.pandas(desc="Filtrando premios válidos")
    df = df[df['award'].notna() & df['award'].progress_apply(is_english_filtered)]

    # Agrupación por artista y consolidación de campos
    df = df.groupby("artist").agg({
        "country": valor_mas_comun,
        "death": valor_mas_comun,
        "gender": valor_mas_comun,
        "award": lambda x: sorted(set(x))
    }).reset_index()

    # Nuevas columnas
    df["award_count"] = df["award"].apply(len)
    df["won_grammy"] = df["award"].apply(
        lambda premios: any("grammy" in str(premio).lower() for premio in premios)
    )
    df["awards_list"] = df["award"].apply(lambda x: "; ".join(x))

    # Reorganizar columnas
    df = df[[
        "artist", "gender", "country", "death", "award_count",
        "won_grammy", "awards_list"
    ]]

    return df