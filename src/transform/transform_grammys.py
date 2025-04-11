import pandas as pd
import re
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(asctime)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

EXCLUDED_CLASSICAL_CATEGORIES = [
    "Best Classical Vocal Soloist Performance", "Best Classical Vocal Performance",
    "Best Small Ensemble Performance (With Or Without Conductor)",
    "Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)",
    "Most Promising New Classical Recording Artist",
    "Best Classical Performance - Vocal Soloist (With Or Without Orchestra)",
    "Best New Classical Artist", "Best Classical Vocal Soloist",
    "Best Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)",
    "Best Classical Performance - Vocal Soloist"
]

TARGET_ROLES = [
    "artist", "artists", "composer", "conductor", "conductor/soloist",
    "choir director", "chorus master", "graphic designer", "soloist",
    "soloists", "ensembles"
]

def parse_artist_from_parentheses(worker_text):
    
    if pd.isna(worker_text):
        return None
    
    match = re.search(r'\((.*?)\)', worker_text)
    return match.group(1) if match else None

def conditionally_shift_worker_to_artist(record):
    
    if pd.isna(record["artist"]) and pd.notna(record["workers"]):
        worker_info = record["workers"]
        if not re.search(r'[;,]', worker_info):
            return worker_info
    return record["artist"]

def get_pre_semicolon_artist(worker_text, excluded_roles):
    
    if pd.isna(worker_text):
        return None
    
    segments = worker_text.split(';')
    first_segment = segments[0].strip()
    if ',' not in first_segment and not any(role in first_segment.lower() for role in excluded_roles):
        return first_segment
    return None

def find_artists_by_role(worker_text, relevant_roles):
    
    if pd.isna(worker_text):
        return None
    
    roles_regex = '|'.join(re.escape(role) for role in relevant_roles)
    regex_pattern = r'([^;]+?)\s*,\s*(?:' + roles_regex + r')'
    found_matches = re.findall(regex_pattern, worker_text, flags=re.IGNORECASE)
    return ", ".join(match.strip() for match in found_matches) if found_matches else None

def transform_grammy_awards_data(input_df):
    
    try:
        rows_start, cols_start = input_df.shape
        logging.info(f"Initiating Grammy data refinement. Input shape: {rows_start}x{cols_start}.")

        refined_df = input_df.rename(columns={"winner": "is_nominated"})
        logging.info("Renamed 'winner' column to 'is_nominated'.")

        columns_to_remove = ["published_at", "updated_at", "img"]
        refined_df = refined_df.drop(columns=columns_to_remove)
        logging.info(f"Removed columns: {', '.join(columns_to_remove)}.")

        refined_df = refined_df.dropna(subset=["nominee"])
        logging.info(f"Removed records with missing 'nominee'. Shape after drop: {refined_df.shape}.")

        records_missing_both = refined_df[refined_df["artist"].isna() & refined_df["workers"].isna()]
        classical_records_to_drop = records_missing_both[records_missing_both["category"].isin(EXCLUDED_CLASSICAL_CATEGORIES)]

        if not classical_records_to_drop.empty:
             refined_df = refined_df.drop(classical_records_to_drop.index)
             logging.info(f"Removed {len(classical_records_to_drop)} records from excluded classical categories with missing artist/worker info.")

        records_missing_both_indices = records_missing_both.drop(classical_records_to_drop.index).index
        if not records_missing_both_indices.empty:
            refined_df.loc[records_missing_both_indices, "artist"] = refined_df.loc[records_missing_both_indices, "nominee"]
            logging.info(f"Populated 'artist' from 'nominee' for {len(records_missing_both_indices)} records where both 'artist' and 'workers' were null.")

        mask_artist_na = refined_df["artist"].isna()
        if mask_artist_na.any():
             refined_df.loc[mask_artist_na, "artist"] = refined_df[mask_artist_na].apply(
                 lambda row: parse_artist_from_parentheses(row["workers"]), axis=1
             )
             logging.info("Attempted to extract artist from parentheses in 'workers' where 'artist' was null.")

        refined_df["artist"] = refined_df.apply(conditionally_shift_worker_to_artist, axis=1)
        logging.info("Checked for simple worker entries to move to 'artist' column.")

        mask_artist_na = refined_df["artist"].isna()
        if mask_artist_na.any():
            refined_df.loc[mask_artist_na, "artist"] = refined_df[mask_artist_na].apply(
                lambda row: get_pre_semicolon_artist(row["workers"], TARGET_ROLES), axis=1
            )
            logging.info("Attempted extraction of artist from first segment of 'workers'.")

        mask_artist_na = refined_df["artist"].isna()
        if mask_artist_na.any():
            refined_df.loc[mask_artist_na, "artist"] = refined_df[mask_artist_na].apply(
                lambda row: find_artists_by_role(row["workers"], TARGET_ROLES), axis=1
            )
            logging.info("Attempted extraction of artist based on specified roles in 'workers'.")

        refined_df = refined_df.dropna(subset=["artist"])
        logging.info(f"Removed records still missing 'artist' after imputation attempts. Shape after drop: {refined_df.shape}.")

        refined_df["artist"] = refined_df["artist"].replace({"(Various Artists)": "Various Artists"})
        logging.info("Standardized '(Various Artists)' entry.")

        refined_df = refined_df.drop(columns=["workers"])
        logging.info("Removed 'workers' column.")

        rows_end, cols_end = refined_df.shape
        logging.info(f"Grammy data refinement finished. Final shape: {rows_end}x{cols_end}.")

        return refined_df

    except KeyError as e:
        logging.error(f"Refinement failed due to missing column: {e}.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during Grammy data refinement: {e}.")
        raise