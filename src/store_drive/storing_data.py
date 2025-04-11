from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from dotenv import load_dotenv
from pathlib import Path
import os
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(asctime)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

env_path = Path(__file__).parent.resolve() / ".env"
load_dotenv(env_path)

config_dir = Path(os.getenv("CONFIG_DIR")).resolve()

client_secrets_file = config_dir / "client_secrets.json"
settings_file = config_dir / "settings.yaml"
credentials_file = config_dir / "saved_credentials.json"
folder_id = os.getenv("FOLDER_ID")

def initialize_google_drive_service():
    try:
        logging.info("Initiating Google Drive connection sequence.")
        gauth = GoogleAuth(settings_file=settings_file)

        if credentials_file.exists():
            gauth.LoadCredentialsFile(credentials_file)
            if gauth.access_token_expired:
                logging.info("Existing token has expired. Requesting refresh.")
                gauth.Refresh()
            else:
                logging.info("Loaded valid credentials from local file.")
        else:
            logging.info("No stored credentials detected. Initiating browser authentication flow.")
            gauth.LoadClientConfigFile(client_secrets_file)
            gauth.LocalWebserverAuth()
            gauth.SaveCredentialsFile(credentials_file)
            logging.info("Browser authentication successful. Credentials have been saved locally.")

        drive_service = GoogleDrive(gauth)
        logging.info("Successfully authenticated and obtained Google Drive service handle.")
        return drive_service

    except Exception as e:
        logging.error(f"Failed during Google Drive authentication: {e}", exc_info=True)
        raise

def upload_dataframe_to_drive(file_title, data_frame):
    drive_service = initialize_google_drive_service()
    if not drive_service:
        logging.error("Could not proceed with upload due to authentication failure.")
        return

    logging.info(f"Preparing to upload '{file_title}' to Google Drive folder ID: {folder_id}")

    try:
        csv_content = data_frame.to_csv(index=False)

        drive_file = drive_service.CreateFile({
            "title": file_title,
            "parents": [{"kind": "drive#fileLink", "id": folder_id}],
            "mimeType": "text/csv"
        })

        drive_file.SetContentString(csv_content)
        drive_file.Upload()

        logging.info(f"Successfully uploaded '{file_title}' to Google Drive.")

    except Exception as e:
        logging.error(f"Error encountered during file upload for '{file_title}': {e}", exc_info=True)
        raise