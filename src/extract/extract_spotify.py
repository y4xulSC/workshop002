import os
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

def extract_spotify_csv(path):
    
    if not os.path.exists(path):
        logging.error(f"Input file path does not exist: {path}")
        raise FileNotFoundError(f"Source file not found at specified location: {path}. Check the absolute path provided.")
    try:
        logging.info(f"Initiating data read from CSV file: {path}.")
        df = pd.read_csv(path)
        logging.info(f"Successfully created DataFrame from {path}.")
        return df
    except Exception as e:
        logging.error(f"Failed to process data extraction from {path}: {e}.")