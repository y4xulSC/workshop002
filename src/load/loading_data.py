from db.sql_alchemy_conn import creating_engine, load_clean_data
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(asctime)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

def load_combined_dataset(dataframe: pd.DataFrame, target_table_name: str) -> None:
    
    logging.info(f"Initiating data persistence process for table '{target_table_name}'.")
    engine = None
    
    try:
        engine = creating_engine()
        logging.debug("Database engine created successfully.")
        
        load_clean_data(engine, dataframe, target_table_name)
        logging.info(f"Data loading operation for '{target_table_name}' attempted.")
    except Exception as e:
        logging.error(f"Failed to persist data into '{target_table_name}': {e}.")