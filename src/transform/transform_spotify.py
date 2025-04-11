import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(asctime)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

def classify_track_duration(duration_ms):
    if duration_ms < 150000:
        return "Short"
    elif 150000 <= duration_ms <= 300000:
        return "Medium"
    else:
        return "Long"

def normalize_track_popularity(popularity_score):
    if popularity_score <= 30:
        return "Niche"
    elif 31 <= popularity_score <= 70:
        return "Moderate"
    else:
        return "Popular"

def estimate_track_mood(valence_score):
    if valence_score <= 0.3:
        return "Melancholic"
    elif 0.31 <= valence_score <= 0.6:
        return "Neutral"
    else:
        return "Upbeat"

def transform_spotify_track_data(df):
    try:
        initial_rows, initial_cols = df.shape
        logging.info(f"Initiating processing for Spotify dataset ({initial_rows} records, {initial_cols} features).")

        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])
            logging.info("Removed 'Unnamed: 0' index column.")

        df = df.dropna().reset_index(drop=True)
        logging.info(f"Removed rows with missing values. Current row count: {df.shape[0]}.")

        df = df.drop_duplicates()
        logging.info(f"Removed exact duplicate entries. Current row count: {df.shape[0]}.")

        df = df.drop_duplicates(subset=["track_id"]).reset_index(drop=True)
        logging.info(f"Ensured track ID uniqueness. Current row count: {df.shape[0]}.")

        genre_map_config = {
            'Rock/Metal': ['alt-rock', 'alternative', 'black-metal', 'death-metal', 'emo', 'grindcore', 'hard-rock', 'hardcore', 'heavy-metal', 'metal', 'metalcore', 'psych-rock', 'punk-rock', 'punk', 'rock-n-roll', 'rock', 'grunge', 'j-rock', 'goth', 'industrial', 'rockabilly', 'indie'],
            'Pop': ['pop', 'indie-pop', 'power-pop', 'k-pop', 'j-pop', 'mandopop', 'cantopop', 'pop-film', 'j-idol', 'synth-pop'],
            'Electronic/Dance': ['edm', 'electro', 'electronic', 'house', 'deep-house', 'progressive-house', 'techno', 'trance', 'dubstep', 'drum-and-bass', 'dub', 'garage', 'idm', 'club', 'dance', 'minimal-techno', 'detroit-techno', 'chicago-house', 'breakbeat', 'hardstyle', 'j-dance', 'trip-hop'],
            'Urban': ['hip-hop', 'r-n-b', 'dancehall', 'reggaeton', 'reggae'],
            'Latin': ['brazil', 'salsa', 'samba', 'spanish', 'pagode', 'sertanejo', 'mpb', 'latin', 'latino'],
            'Global Sounds': ['indian', 'iranian', 'malay', 'turkish', 'tango', 'afrobeat', 'french', 'german', 'british', 'swedish'],
            'Jazz/Soul/Funk': ['blues', 'bluegrass', 'funk', 'gospel', 'jazz', 'soul', 'groove', 'disco', 'ska'],
            'Thematic': ['children', 'disney', 'forro', 'kids', 'party', 'romance', 'show-tunes', 'comedy', 'anime'],
            'Instrumental/Ambient': ['acoustic', 'classical', 'guitar', 'piano', 'world-music', 'opera', 'new-age', 'ambient', 'chill', 'sleep', 'study'],
            'Folk/Country': ['country', 'honky-tonk', 'folk', 'singer-songwriter'],
            'MoodBased': ['happy', 'sad']
        }
        detailed_to_broad_genre = {genre: category for category, genres in genre_map_config.items() for genre in genres}
        df['genre_category'] = df['track_genre'].map(detailed_to_broad_genre).fillna('Other')
        logging.info("Mapped detailed genres to broader categories.")

        core_track_attributes = ['track_name', 'artists', 'danceability', 'energy', 'explicit', 'popularity', 'track_genre']
        df = df.drop_duplicates(subset=core_track_attributes, keep='first')
        logging.info(f"Removed duplicates based on core attributes. Current row count: {df.shape[0]}.")

        df = (df
                .sort_values(by="popularity", ascending=False)
                .groupby(["track_name", "artists"], group_keys=False)
                .head(1)
                .sort_index()
                .reset_index(drop=True))
        logging.info(f"Retained most popular version per track/artist combination. Current row count: {df.shape[0]}.")

        df = df.assign(
            duration_minutes = lambda x: (x["duration_ms"] // 60000).astype(int),
            duration_label = lambda x: x["duration_ms"].apply(classify_track_duration),
            popularity_tier = lambda x: x["popularity"].apply(normalize_track_popularity),
            inferred_mood = lambda x: x["valence"].apply(estimate_track_mood),
            likely_live = lambda x: x["liveness"] > 0.8
        )
        logging.info("Generated derived features: duration_minutes, duration_label, popularity_tier, inferred_mood, likely_live.")

        columns_to_discard = [
            "loudness", "mode", "duration_ms", "key", "tempo", "valence",
            "speechiness", "acousticness", "instrumentalness", "liveness",
            "time_signature", "track_genre"
        ]
        df = df.drop(columns=columns_to_discard)
        logging.info(f"Removed source feature columns: {', '.join(columns_to_discard)}.")

        final_rows, final_cols = df.shape
        logging.info(f"Spotify data processing finished. Final dataset dimensions: {final_rows}x{final_cols}.")

        return df

    except KeyError as e:
        logging.error(f"Data processing failed due to missing column: {e}.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during Spotify data processing: {e}.")
        raise