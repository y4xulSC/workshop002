from db.sql_alchemy_conn import creating_engine

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

def extract_grammys_db():
    
    engine = creating_engine()
    
    try:
        logging.info("Querying the 'grammy_awards' table.")
        df = pd.read_sql_table("grammy_awards", engine)
        logging.info("Successfully fetched data from 'grammy_awards'.")
        return df
    except Exception as e:
        logging.error(f"Failed to fetch data from 'grammy_awards': {e}.")