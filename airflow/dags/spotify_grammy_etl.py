from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.drive.transfers.local_to_drive import LocalFilesystemToGoogleDriveOperator
import pandas as pd
import requests
import psycopg2
import os

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_spotify_data():
    # Read Spotify dataset from CSV
    spotify_df = pd.read_csv('spotify_dataset.csv')
    
    # Basic cleaning
    spotify_df = spotify_df.drop_duplicates()
    spotify_df = spotify_df.dropna(subset=['track_name'])
    
    # Save to temporary file for next task
    spotify_df.to_csv('/tmp/spotify_clean.csv', index=False)

def extract_grammy_data():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="music_data",
        user="airflow",
        password="airflow",
        host="localhost"
    )
    
    # Query grammy data (assuming it's already loaded)
    grammy_df = pd.read_sql("SELECT * FROM grammy_awards", conn)
    conn.close()
    
    # Basic cleaning
    grammy_df = grammy_df.drop_duplicates()
    grammy_df['artist'] = grammy_df['artist'].str.strip()
    
    # Save to temporary file
    grammy_df.to_csv('/tmp/grammy_clean.csv', index=False)

def extract_spotify_api_data():
    # Example: Get artist popularity data from Spotify API
    # Note: You would need actual API credentials for a real implementation
    headers = {
        "Authorization": "Bearer YOUR_SPOTIFY_API_TOKEN"
    }
    
    # Get top artists (mock implementation)
    api_data = []
    for artist in ['Taylor Swift', 'Drake', 'The Weeknd']:
        response = requests.get(
            f"https://api.spotify.com/v1/search?q={artist}&type=artist",
            headers=headers
        )
        if response.status_code == 200:
            artist_data = response.json()['artists']['items'][0]
            api_data.append({
                'artist_name': artist,
                'popularity': artist_data['popularity'],
                'followers': artist_data['followers']['total'],
                'genres': ','.join(artist_data['genres'])
            })
    
    api_df = pd.DataFrame(api_data)
    api_df.to_csv('/tmp/spotify_api_data.csv', index=False)

def transform_and_merge_data():
    # Load all cleaned data
    spotify_df = pd.read_csv('/tmp/spotify_clean.csv')
    grammy_df = pd.read_csv('/tmp/grammy_clean.csv')
    api_df = pd.read_csv('/tmp/spotify_api_data.csv')
    
    # Transform Spotify data - aggregate by artist
    artist_stats = spotify_df.groupby('artist_name').agg({
        'danceability': 'mean',
        'energy': 'mean',
        'loudness': 'mean',
        'speechiness': 'mean',
        'acousticness': 'mean',
        'instrumentalness': 'mean',
        'liveness': 'mean',
        'valence': 'mean',
        'tempo': 'mean',
        'duration_ms': 'mean',
        'popularity': 'mean'
    }).reset_index()
    
    # Transform Grammy data - count awards per artist
    grammy_wins = grammy_df[grammy_df['winner'] == True]
    grammy_counts = grammy_wins.groupby('artist').size().reset_index(name='grammy_wins')
    
    # Merge all data
    merged_df = artist_stats.merge(
        grammy_counts,
        left_on='artist_name',
        right_on='artist',
        how='left'
    ).merge(
        api_df,
        left_on='artist_name',
        right_on='artist_name',
        how='left'
    )
    
    # Fill NA values
    merged_df['grammy_wins'] = merged_df['grammy_wins'].fillna(0)
    
    # Save merged data
    merged_df.to_csv('/tmp/merged_music_data.csv', index=False)

def load_to_database():
    conn = psycopg2.connect(
        dbname="music_data",
        user="airflow",
        password="airflow",
        host="localhost"
    )
    cursor = conn.cursor()
    
    # Create table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS music_analytics (
        artist_name VARCHAR(255) PRIMARY KEY,
        danceability FLOAT,
        energy FLOAT,
        loudness FLOAT,
        speechiness FLOAT,
        acousticness FLOAT,
        instrumentalness FLOAT,
        liveness FLOAT,
        valence FLOAT,
        tempo FLOAT,
        duration_ms FLOAT,
        spotify_popularity FLOAT,
        grammy_wins INT,
        api_popularity FLOAT,
        followers FLOAT,
        genres TEXT
    )
    """)
    
    # Load data
    merged_df = pd.read_csv('/tmp/merged_music_data.csv')
    for _, row in merged_df.iterrows():
        cursor.execute("""
        INSERT INTO music_analytics VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (artist_name) DO UPDATE SET
            danceability = EXCLUDED.danceability,
            energy = EXCLUDED.energy,
            loudness = EXCLUDED.loudness,
            speechiness = EXCLUDED.speechiness,
            acousticness = EXCLUDED.acousticness,
            instrumentalness = EXCLUDED.instrumentalness,
            liveness = EXCLUDED.liveness,
            valence = EXCLUDED.valence,
            tempo = EXCLUDED.tempo,
            duration_ms = EXCLUDED.duration_ms,
            spotify_popularity = EXCLUDED.spotify_popularity,
            grammy_wins = EXCLUDED.grammy_wins,
            api_popularity = EXCLUDED.api_popularity,
            followers = EXCLUDED.followers,
            genres = EXCLUDED.genres
        """, tuple(row))
    
    conn.commit()
    conn.close()

# Define DAG
with DAG(
    'spotify_grammy_etl',
    default_args=default_args,
    description='ETL pipeline for Spotify and Grammy data',
    schedule_interval='@weekly',
    catchup=False
) as dag:
    
    extract_spotify_task = PythonOperator(
        task_id='extract_spotify_data',
        python_callable=extract_spotify_data
    )
    
    extract_grammy_task = PythonOperator(
        task_id='extract_grammy_data',
        python_callable=extract_grammy_data
    )
    
    extract_api_task = PythonOperator(
        task_id='extract_spotify_api_data',
        python_callable=extract_spotify_api_data
    )
    
    transform_task = PythonOperator(
        task_id='transform_and_merge_data',
        python_callable=transform_and_merge_data
    )
    
    load_db_task = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database
    )
    
    load_gdrive_task = LocalFilesystemToGoogleDriveOperator(
        task_id='load_to_gdrive',
        local_path='/tmp/merged_music_data.csv',
        remote_path='music_data/',
        gdrive_conn_id='google_cloud_default',
        gcp_conn_id='google_cloud_default'
    )
    
    # Set dependencies
    [extract_spotify_task, extract_grammy_task, extract_api_task] >> transform_task
    transform_task >> [load_db_task, load_gdrive_task]