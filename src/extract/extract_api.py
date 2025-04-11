import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import os
import math
import logging
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

def extract_spotify_api(input_csv_path: str = "./data/spotify_dataset.csv",
                               artist_limit: int = 100,
                               track_batch_size: int = 50,
                               artist_batch_size: int = 50) -> pd.DataFrame:

    try:
        env_file_path = os.path.join(os.path.dirname(__file__), '.env')
        load_dotenv(dotenv_path=env_file_path)
    except Exception as e:
         logging.error(f"Error al cargar el archivo .env: {e}")

    spotify_client_id = os.getenv("SPOTIFY_CLIENT_ID")
    spotify_client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

    if not spotify_client_id or not spotify_client_secret:
        logging.error("SPOTIFY_CLIENT_ID o SPOTIFY_CLIENT_SECRET no encontrados.")
        logging.error("Asegúrate de que estén definidos en tu archivo .env o variables de entorno.")
        return pd.DataFrame()

    sp = None
    try:
        auth_manager = SpotifyClientCredentials(client_id=spotify_client_id,
                                                client_secret=spotify_client_secret)
        sp = spotipy.Spotify(auth_manager=auth_manager)
        sp.search(q='test', type='track', limit=1)
        logging.info("Autenticación con Spotify exitosa.")
    except Exception as e:
        logging.error(f"Fallo en la autenticación con Spotify: {e}")
        return pd.DataFrame()

    track_ids = []
    try:
        logging.info(f"Leyendo archivo de entrada: {input_csv_path}...")
        df_input = pd.read_csv(input_csv_path)
        if 'track_id' not in df_input.columns:
            logging.error(f"Columna 'track_id' no encontrada en {input_csv_path}")
            return pd.DataFrame()

        track_ids = df_input['track_id'].dropna().unique().tolist()
        logging.info(f"Se encontraron {len(track_ids)} track_ids únicos y válidos en {input_csv_path}.")

        if not track_ids:
            logging.warning("No se encontraron track_ids válidos en el archivo de entrada.")
            return pd.DataFrame()

    except FileNotFoundError:
        logging.error(f"Archivo de entrada no encontrado: {input_csv_path}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error al leer el archivo CSV '{input_csv_path}': {e}")
        return pd.DataFrame()

    unique_artist_ids = set()
    logging.info(f"Procesando {len(track_ids)} track_ids para obtener artist_ids únicos...")
    total_batches = math.ceil(len(track_ids) / track_batch_size)
    processed_tracks = 0

    for i in range(0, len(track_ids), track_batch_size):
        batch_track_ids = track_ids[i:i + track_batch_size]
        current_batch_num = (i // track_batch_size) + 1

        if total_batches <= 20 or current_batch_num % (max(1, total_batches // 20)) == 0 or current_batch_num == total_batches:
             logging.info(f"Procesando lote de tracks {current_batch_num}/{total_batches}...")

        try:
            results = sp.tracks(batch_track_ids)
            if results and 'tracks' in results:
                valid_tracks = [t for t in results['tracks'] if t]
                processed_tracks += len(valid_tracks)
                for track_info in valid_tracks:
                    if 'artists' in track_info:
                        for artist in track_info['artists']:
                            if artist and 'id' in artist and artist['id']:
                               unique_artist_ids.add(artist['id'])

        except spotipy.SpotifyException as e:
            logging.error(f"Error de API Spotify en lote de tracks (ID: {batch_track_ids[0] if batch_track_ids else 'N/A'}): Estado {e.http_status} - {e.msg}. Saltando lote.")
            if e.http_status == 429:
                try:
                    retry_after = int(e.headers.get('Retry-After', 5))
                    logging.warning(f"Límite de tasa alcanzado. Esperando {retry_after} segundos...")
                    time.sleep(retry_after)
                except ValueError:
                     logging.warning("Límite de tasa alcanzado. No se pudo leer Retry-After. Esperando 5 segundos...")
                     time.sleep(5)
        except Exception as e:
            logging.error(f"Error general en lote de tracks (ID: {batch_track_ids[0] if batch_track_ids else 'N/A'}): {type(e).__name__} - {e}. Saltando lote.")
            time.sleep(1)

    artist_ids_list_all = list(unique_artist_ids)
    logging.info(f"Se procesaron {processed_tracks} tracks válidos.")
    logging.info(f"Se encontraron {len(artist_ids_list_all)} artist_ids únicos en total.")

    artist_ids_list_to_process = []
    if len(artist_ids_list_all) > artist_limit:
        artist_ids_list_to_process = artist_ids_list_all[:artist_limit]
        logging.info(f"Limitando el procesamiento de detalles a los primeros {artist_limit} artistas únicos encontrados.")
    else:
        artist_ids_list_to_process = artist_ids_list_all
        logging.info(f"Procesando detalles para los {len(artist_ids_list_to_process)} artistas únicos encontrados.")

    artist_data = []
    if not artist_ids_list_to_process:
        logging.warning("No hay artist_ids para procesar.")
    else:
        logging.info(f"Obteniendo detalles para {len(artist_ids_list_to_process)} artistas...")
        total_artist_batches = math.ceil(len(artist_ids_list_to_process) / artist_batch_size)

        for i in range(0, len(artist_ids_list_to_process), artist_batch_size):
            batch_artist_ids = artist_ids_list_to_process[i:i + artist_batch_size]
            current_artist_batch_num = (i // artist_batch_size) + 1
            logging.info(f"Procesando lote de artistas {current_artist_batch_num}/{total_artist_batches}...")

            try:
                results = sp.artists(batch_artist_ids)
                if results and 'artists' in results:
                    valid_artists = [a for a in results['artists'] if a]
                    for artist_info in valid_artists:
                        artist_details = {
                            'artist_id': artist_info.get('id'),
                            'artist_name': artist_info.get('name'),
                            'followers': artist_info.get('followers', {}).get('total'),
                            'popularity': artist_info.get('popularity'),
                            'genres': ', '.join(artist_info.get('genres', []))
                        }
                        if artist_details['artist_id']:
                            artist_data.append(artist_details)

            except spotipy.SpotifyException as e:
                logging.error(f"Error de API Spotify en lote de artistas (ID: {batch_artist_ids[0] if batch_artist_ids else 'N/A'}): Estado {e.http_status} - {e.msg}. Saltando lote.")
                if e.http_status == 429:
                    try:
                        retry_after = int(e.headers.get('Retry-After', 5))
                        logging.warning(f"Límite de tasa alcanzado. Esperando {retry_after} segundos...")
                        time.sleep(retry_after)
                    except ValueError:
                        logging.warning("Límite de tasa alcanzado. No se pudo leer Retry-After. Esperando 5 segundos...")
                        time.sleep(5)
            except Exception as e:
                logging.error(f"Error general en lote de artistas (ID: {batch_artist_ids[0] if batch_artist_ids else 'N/A'}): {type(e).__name__} - {e}. Saltando lote.")
                time.sleep(1)

    if artist_data:
        logging.info(f"Se recolectaron datos exitosamente para {len(artist_data)} artistas.")
        df_output = pd.DataFrame(artist_data)
        return df_output
    else:
        logging.warning("No se recolectaron datos válidos de artistas.")
        return pd.DataFrame()