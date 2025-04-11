from extract.extract_api import extract_spotify_api
from extract.extract_spotify import extract_spotify_csv
from extract.extract_grammys import extract_grammys_db

from transform.transform_api import *
from transform.transform_spotify import transform_spotify_track_data
from transform.transform_grammys import transform_grammy_awards_data

from merge.merging_data import merge_all_data

from load.loading_data import load_combined_dataset

from store_drive.storing_data import upload_dataframe_to_drive

import json
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(asctime)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def extract_api():
    try:
        df = extract_spotify_api()
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error during Spotify API data extraction: {e}")
        raise

def extract_spotify():
    try:
        df = extract_spotify_csv("./data/spotify_dataset.csv")
        logging.info("Spotify data extraction completed.")
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error during Spotify data extraction: {e}")
        raise

def extract_grammys():
    try:
        df = extract_grammys_db("./data/the_grammy_awards.csv")
        logging.info("Grammy data extraction completed.")
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error during Grammy data extraction: {e}")
        raise
    
def transform_api(df_json):
    try:
        json_data = json.loads(df_json)
        raw_df = pd.DataFrame(json_data)
        transformed_df = transform_api_data(raw_df)

        return transformed_df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error during API data transformation: {e}")
        raise

def transform_spotify(df_json):
    try:
        json_data = json.loads(df_json)
        raw_df = pd.DataFrame(json_data)
        transformed_df = transform_spotify_track_data(raw_df)

        return transformed_df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error during Spotify data transformation: {e}")
        raise

def transform_grammys(df_json):
    try:

        json_data = json.loads(df_json)
        raw_df = pd.DataFrame(json_data)
        transformed_df = transform_grammy_awards_data(raw_df)

        return transformed_df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error during Grammy data transformation: {e}")
        raise

def merge_data(spotify_df_json, grammys_df_json, api_df_json):
    try:
        api_data = pd.read_json(api_df_json, orient="records")
        spotify_data = pd.read_json(spotify_df_json, orient="records")
        grammys_data = pd.read_json(grammys_df_json, orient="records")
        merged_df = merge_all_data(spotify_data, grammys_data, api_data)

        return merged_df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error during data merging: {e}")
        raise

def load_data(df_json):
    try:

        data_to_load = pd.read_json(df_json, orient="records")
        load_combined_dataset(data_to_load, "merged_data")

        return df_json
    except Exception as e:
        logging.error(f"Error during data loading: {e}")
        raise

def store_data(df_json):
    try:
        data_to_store = pd.read_json(df_json, orient="records")
        upload_dataframe_to_drive("merged_data.csv", data_to_store)
    except Exception as e:
        logging.error(f"Error during data storage: {e}")
        raise