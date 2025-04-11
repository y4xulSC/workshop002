import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(asctime)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# --- Funciones Auxiliares ---
def impute_missing_values(dataframe, target_columns, fill_value):
    for col in target_columns:
        if col in dataframe.columns:
            dataframe[col] = dataframe[col].fillna(fill_value)
        else:
            logging.warning(f"Columna '{col}' no encontrada para imputación. Omitiendo.")
    return dataframe

def remove_data_columns(dataframe, columns_to_remove):
    present_columns = [col for col in columns_to_remove if col in dataframe.columns]
    if present_columns:
        dataframe = dataframe.drop(columns=present_columns).copy()
        logging.debug(f"Columnas eliminadas: {', '.join(present_columns)}.")
    return dataframe


# --- Función de Merge Adaptada ---
def merge_all_data(spotify_data: pd.DataFrame,
                   grammy_awards_data: pd.DataFrame,
                   api_data: pd.DataFrame) -> pd.DataFrame:

    logging.info("Iniciando integración de datos: Spotify (CSV), Grammys y Spotify (API).")

    # --- Validación de Entradas ---
    if spotify_data is None or spotify_data.empty:
        logging.error("El DataFrame de Spotify (CSV) está vacío o es None. No se puede continuar.")
        return pd.DataFrame()
    if grammy_awards_data is None or grammy_awards_data.empty:
        logging.warning("El DataFrame de Grammys está vacío o es None. Se procederá sin datos de Grammys.")
    if api_data is None or api_data.empty:
        logging.warning("El DataFrame de la API está vacío o es None. Se procederá sin datos de followers.")

    logging.info(f"Dimensiones Spotify (CSV): {spotify_data.shape[0]} filas, {spotify_data.shape[1]} columnas.")
    if grammy_awards_data is not None:
        logging.info(f"Dimensiones Grammys: {grammy_awards_data.shape[0]} filas, {grammy_awards_data.shape[1]} columnas.")
    if api_data is not None:
        logging.info(f"Dimensiones API (followers): {api_data.shape[0]} filas, {api_data.shape[1]} columnas.")

    # --- Etapa 1: Merge Spotify (CSV) + Grammys ---
    try:
        if 'track_name' not in spotify_data.columns:
             logging.error("Falta la columna 'track_name' en spotify_data para el merge con Grammys.")
             return pd.DataFrame()

        if grammy_awards_data is not None and not grammy_awards_data.empty:
            if 'nominee' not in grammy_awards_data.columns:
                logging.error("Falta la columna 'nominee' en grammy_awards_data para el merge.")
                combined_data_stage1 = spotify_data.copy()
                logging.warning("Continuando sin datos de Grammys debido a columna faltante.")
            else:
                logging.info("Realizando merge entre Spotify (CSV) y Grammys.")
                spotify_data['spotify_join_key'] = spotify_data['track_name'].astype(str).str.lower().str.strip()
                grammy_awards_data['grammy_join_key'] = grammy_awards_data['nominee'].astype(str).str.lower().str.strip()
                logging.debug("Claves temporales normalizadas creadas para join Spotify-Grammys.")

                combined_data_stage1 = pd.merge(
                    spotify_data,
                    grammy_awards_data,
                    how="left",
                    left_on="spotify_join_key",
                    right_on="grammy_join_key",
                    suffixes=("_spotify", "_grammy")
                )
                logging.info(f"Merge Spotify-Grammys realizado. Forma resultante: {combined_data_stage1.shape}.")

                cols_to_impute_text = ["title", "category"]
                combined_data_stage1 = impute_missing_values(combined_data_stage1, cols_to_impute_text, "No Aplicable")
                cols_to_impute_bool = ["is_nominated"]
                combined_data_stage1 = impute_missing_values(combined_data_stage1, cols_to_impute_bool, False)

                cols_to_drop_stage1 = [
                    "year", "artist", "nominee",
                    "spotify_join_key", "grammy_join_key"
                ]
                combined_data_stage1 = remove_data_columns(combined_data_stage1, cols_to_drop_stage1)
        else:
            combined_data_stage1 = spotify_data.copy()
            logging.info("Omitiendo merge con Grammys (datos no proporcionados o vacíos).")

    except KeyError as e:
        logging.error(f"Merge Spotify-Grammys falló por columna clave faltante: {e}")
        raise
    except Exception as e:
        logging.error(f"Error inesperado durante merge Spotify-Grammys: {e}")
        raise

    # --- Etapa 2: Merge (Resultado Etapa 1) + API (Followers) ---
    try:
        artist_col_spotify = 'artists_spotify' if 'artists_spotify' in combined_data_stage1.columns else 'artists'
        if artist_col_spotify not in combined_data_stage1.columns:
             logging.error(f"Falta la columna de artista ('{artist_col_spotify}') en los datos combinados para el merge con API.")
             api_data = None
             logging.warning("Continuando sin datos de followers debido a columna de artista faltante.")

        if api_data is not None and not api_data.empty:
             if 'artist_name' not in api_data.columns or 'followers' not in api_data.columns:
                  logging.error("Faltan 'artist_name' o 'followers' en api_data para el merge.")
                  logging.warning("Continuando sin datos de followers debido a columnas faltantes en datos de API.")
             else:
                logging.info("Realizando merge entre datos combinados y datos de API (followers).")

                combined_data_stage1['combined_join_key'] = combined_data_stage1[artist_col_spotify].astype(str).str.split(',').str[0].str.lower().str.strip()
                api_data['api_join_key'] = api_data['artist_name'].astype(str).str.lower().str.strip()
                logging.debug("Claves temporales normalizadas creadas para join con API (usando primer artista).")

                api_data_subset = api_data[['api_join_key', 'followers']].copy()
                api_data_subset = api_data_subset.drop_duplicates(subset=['api_join_key'], keep='first')

                combined_data_stage2 = pd.merge(
                    combined_data_stage1,
                    api_data_subset,
                    how="left",
                    left_on="combined_join_key",
                    right_on="api_join_key"
                )
                logging.info(f"Merge con API realizado. Forma resultante: {combined_data_stage2.shape}.")

                combined_data_stage2 = impute_missing_values(combined_data_stage2, ['followers'], 0)

                cols_to_drop_stage2 = ["combined_join_key"]
                if 'api_join_key' in combined_data_stage2.columns:
                    cols_to_drop_stage2.append('api_join_key')

                combined_data_stage2 = remove_data_columns(combined_data_stage2, cols_to_drop_stage2)
                final_combined_data = combined_data_stage2

        else:
            logging.info("Omitiendo merge con API (datos no proporcionados, vacíos o columnas clave faltantes).")
            final_combined_data = combined_data_stage1.copy()
            if 'followers' not in final_combined_data.columns:
                 final_combined_data['followers'] = 0
                 logging.debug("Añadida columna 'followers' con valor 0 por defecto.")

    except KeyError as e:
        logging.error(f"Merge con API falló por columna clave faltante: {e}")
        raise
    except Exception as e:
        logging.error(f"Error inesperado durante merge con API: {e}")
        raise

    # --- Limpieza Final y Creación de ID ---
    try:
        final_combined_data = final_combined_data.reset_index(drop=True)
        final_combined_data.insert(0, 'record_id', final_combined_data.index)
        final_combined_data['record_id'] = final_combined_data['record_id'].astype(int)
        logging.debug("Añadida columna 'record_id' basada en el índice final.")

        logging.info(f"Integración de datos finalizada. El dataset final contiene {final_combined_data.shape[0]} registros y {final_combined_data.shape[1]} características.")
        return final_combined_data

    except Exception as e:
         logging.error(f"Error durante la limpieza final o creación de record_id: {e}")
         return final_combined_data if 'final_combined_data' in locals() else pd.DataFrame()