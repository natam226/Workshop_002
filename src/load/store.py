import os
import logging
import pickle
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SCOPES = ['https://www.googleapis.com/auth/drive.file']

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
CREDENTIALS_PATH = os.path.join(BASE_PATH, 'credentialsdrive.json')
TOKEN_PATH = "token.pickle"


def authenticate_drive(credentials_path: str = CREDENTIALS_PATH, token_path: str = TOKEN_PATH):
    """
    Authenticates and returns a Google Drive API service instance.

    Args:
        credentials_path (str): Path to the client credentials JSON file.
        token_path (str): Path to the token file for storing authentication tokens.

    Returns:
        googleapiclient.discovery.Resource: Authenticated Google Drive API service.
    """
    creds = None

    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credential file not found at: {credentials_path}")

    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            try:
                logging.info("Opening browser for authentication...")
                creds = flow.run_local_server(port=8080)
            except Exception as e:
                logging.warning(f"Browser authentication failed: {e}")
                logging.info("Falling back to console authentication...")
                creds = flow.run_console()

        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)


def upload_file_to_drive(filepath: str, filename: str = None):
    """
    Uploads a file to Google Drive.

    Args:
        filepath (str): Full path to the file to be uploaded.
        filename (str, optional): Desired name for the file on Drive.
                                  If None, the local filename will be used.

    Returns:
        None
    """
    service = authenticate_drive()
    file_metadata = {'name': filename or os.path.basename(filepath)}
    media = MediaFileUpload(filepath, resumable=True)

    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        logging.info(f"File uploaded to Google Drive with ID: {file.get('id')}")
    except Exception as e:
        logging.error(f"Error uploading file to Google Drive: {e}")
