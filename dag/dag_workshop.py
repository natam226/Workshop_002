import os
import sys
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# === Agregar la carpeta src al path ===
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# === Importar funciones ===
from src.extract.extract_api import extract_api
from src.extract.extract_grammy import extract_data as extract_grammy
from src.extract.extract_spotify import extract_spotify_data

from src.transform.transform_api import transformation_api
from src.transform.transform_grammy import transform_grammy_data
from src.transform.transform_spotify import transform_spotify_data

from src.transform.merge import merge_datasets
from src.load.load import load_to_postgresql
from src.load.store import upload_file_to_drive


# === Configuración del DAG ===
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    dag_id='etl_artistas_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL completo para datos de artistas (Spotify, Grammy, API Wikidata)'
)

# === Rutas temporales ===
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_TEMP_DIR = os.path.join(BASE_DIR, 'data_temp')
os.makedirs(DATA_TEMP_DIR, exist_ok=True)

SPOTIFY_PATH = os.path.join(DATA_TEMP_DIR, 'spotify.csv')
GRAMMY_PATH = os.path.join(DATA_TEMP_DIR, 'grammy.csv')
API_PATH = os.path.join(DATA_TEMP_DIR, 'api.csv')
MERGED_PATH = os.path.join(DATA_TEMP_DIR, 'merged.csv')

# === TAREAS ===

# 🔽 EXTRACT
def task_extract_spotify():
    df = extract_spotify_data()
    if df.empty:
        raise ValueError("❌ El DataFrame de Spotify está vacío.")
    df.to_csv(SPOTIFY_PATH, index=False)
    logging.info(f"✅ Spotify extraído en {SPOTIFY_PATH}")

def task_extract_grammy():
    df = extract_grammy()
    if df.empty:
        raise ValueError("❌ El DataFrame de Grammy está vacío.")
    df.to_csv(GRAMMY_PATH, index=False)
    logging.info(f"✅ Grammy extraído en {GRAMMY_PATH}")

def task_extract_api():
    df = extract_api()
    if df.empty:
        raise ValueError("❌ El DataFrame de Wikidata está vacío.")
    df.to_csv(API_PATH, index=False)
    logging.info(f"✅ API extraído en {API_PATH}")

# 🔄 TRANSFORM
def task_transform_spotify():
    df = pd.read_csv(SPOTIFY_PATH)
    df_clean = transform_spotify_data(df)
    df_clean.to_csv(SPOTIFY_PATH, index=False)
    logging.info(f"✅ Spotify transformado en {SPOTIFY_PATH}")

def task_transform_grammy():
    df = pd.read_csv(GRAMMY_PATH)
    df_clean = transform_grammy_data(df)
    df_clean.to_csv(GRAMMY_PATH, index=False)
    logging.info(f"✅ Grammy transformado en {GRAMMY_PATH}")

def task_transform_api():
    df = pd.read_csv(API_PATH)
    df_clean = transformation_api(df)
    df_clean.to_csv(API_PATH, index=False)
    logging.info(f"✅ API transformado en {API_PATH}")

# 🧬 MERGE
def task_merge():
    df_spotify = pd.read_csv(SPOTIFY_PATH)
    df_grammy = pd.read_csv(GRAMMY_PATH)
    df_api = pd.read_csv(API_PATH)
    df_merged = merge_datasets(df_spotify, df_grammy, df_api)
    df_merged.to_csv(MERGED_PATH, index=False)
    logging.info(f"✅ Datos combinados en {MERGED_PATH}")

# 🚀 LOAD
def task_load():
    df = pd.read_csv(MERGED_PATH)
    load_to_postgresql(df, "data_pipeline")
    logging.info("✅ Datos cargados exitosamente a la base de datos")

def task_store_to_drive():
    upload_file_to_drive(MERGED_PATH, filename="artistas_merge.csv")
    logging.info("✅ Archivo subido a Google Drive desde DAG")

# === OPERADORES ===

extract_spotify_op = PythonOperator(task_id='extract_spotify', python_callable=task_extract_spotify, dag=dag)
extract_grammy_op = PythonOperator(task_id='extract_grammy', python_callable=task_extract_grammy, dag=dag)
extract_api_op = PythonOperator(task_id='extract_api', python_callable=task_extract_api, dag=dag)

transform_spotify_op = PythonOperator(task_id='transform_spotify', python_callable=task_transform_spotify, dag=dag)
transform_grammy_op = PythonOperator(task_id='transform_grammy', python_callable=task_transform_grammy, dag=dag)
transform_api_op = PythonOperator(task_id='transform_api', python_callable=task_transform_api, dag=dag)

merge_op = PythonOperator(task_id='merge_datasets', python_callable=task_merge, dag=dag)
load_op = PythonOperator(task_id='load_to_postgres', python_callable=task_load, dag=dag)
store_op = PythonOperator(task_id='store_to_drive', python_callable=task_store_to_drive, dag=dag)


# === DEPENDENCIAS ===

extract_spotify_op >> transform_spotify_op
extract_grammy_op >> transform_grammy_op
extract_api_op >> transform_api_op

[transform_spotify_op, transform_grammy_op, transform_api_op] >> merge_op >> load_op

load_op >> store_op
