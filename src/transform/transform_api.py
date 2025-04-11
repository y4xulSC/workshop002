import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def transform_api_data(df_api: pd.DataFrame) -> pd.DataFrame:

    required_columns = ['artist_name', 'followers']
    logging.info("Iniciando transformación de datos de la API.")

    if df_api is None or df_api.empty:
        logging.warning("El DataFrame de entrada (API) está vacío o es None. Se devolverá un DataFrame vacío.")
        return pd.DataFrame(columns=required_columns)

    try:
        missing_cols = [col for col in required_columns if col not in df_api.columns]
        if missing_cols:
            logging.error(f"Columnas requeridas faltantes en el DataFrame de entrada (API): {', '.join(missing_cols)}")
            return pd.DataFrame(columns=required_columns)

        transformed_df = df_api[required_columns].copy()

        logging.info("Transformación de datos de la API completada exitosamente.")
        return transformed_df

    except KeyError as ke:
         logging.error(f"Error de clave durante la transformación (verificar columnas): {ke}")
         return pd.DataFrame(columns=required_columns)
    except Exception as e:
        logging.error(f"Error inesperado durante la transformación de datos de la API: {e}")
        return pd.DataFrame(columns=required_columns)