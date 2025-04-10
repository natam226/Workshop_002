import os
import logging
import pickle
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Scopes necesarios
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Ajusta estas rutas si las quieres pasar directamente como argumentos
CREDENTIALS_PATH = "credentials.json"
TOKEN_PATH = "token.pickle"

def authenticate_drive(credentials_path=CREDENTIALS_PATH, token_path=TOKEN_PATH):
    creds = None

    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"❌ No se encontró el archivo de credenciales en: {credentials_path}")

    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            try:
                logging.info("🌐 Abriendo navegador para autenticación...")
                creds = flow.run_local_server(port=8080)
            except Exception as e:
                logging.warning(f"⚠️ Falló autenticación en navegador: {e}")
                logging.info("🖥️ Probando autenticación por consola...")
                creds = flow.run_console()

        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)


def upload_file_to_drive(filepath: str, filename: str = None, folder_id: str = None):
    service = authenticate_drive()

    file_metadata = {'name': filename or os.path.basename(filepath)}
    if folder_id:
        file_metadata['parents'] = [folder_id]

    media = MediaFileUpload(filepath, resumable=True)

    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        logging.info(f"✅ Archivo subido exitosamente a Drive con ID: {file.get('id')}")
    except Exception as e:
        logging.error(f"❌ Error al subir archivo a Google Drive: {e}")
