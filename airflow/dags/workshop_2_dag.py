from datetime import datetime, timedelta
from airflow.decorators import dag, task
from task_etl import *

default_args = {
    'owner': "airflow",
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 14),
    'email': ["admin@example.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='ETL pipeline processing Spotify and Grammy data.',
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    concurrency=4,
    tags=['etl', 'spotify', 'grammys']
)
def create_spotify_grammy_etl():
    
    @task
    def task_extract_api():
        return extract_api()
    
    api_raw_json = task_extract_api()

    @task
    def task_extract_spotify():
        return extract_spotify()

    spotify_raw_json = task_extract_spotify()

    @task
    def task_extract_grammys():
        return extract_grammys()

    grammys_raw_json = task_extract_grammys()
    
    @task
    def task_transform_api(raw_json_data):
        return transform_api(raw_json_data)
    
    api_transformed_json = task_transform_api(api_raw_json)

    @task
    def task_transform_spotify(raw_json_data):
        return transform_spotify(raw_json_data)

    spotify_transformed_json = task_transform_spotify(spotify_raw_json)

    @task
    def task_transform_grammys(raw_json_data):
        return transform_grammys(raw_json_data)

    grammys_transformed_json = task_transform_grammys(grammys_raw_json)

    @task
    def task_merge_datasets(spotify_json, grammys_json, api_json):
        return merge_data(spotify_json, grammys_json, api_json)

    merged_data_json = task_merge_datasets(spotify_transformed_json, grammys_transformed_json, api_transformed_json)

    @task
    def task_load_to_database(data_json):
        return load_data(data_json)

    loaded_data_confirmation = task_load_to_database(merged_data_json)

    @task
    def task_store_to_drive(data_json):
        store_data(data_json)

    task_store_to_drive(loaded_data_confirmation)

spotify_grammy_etl_dag = create_spotify_grammy_etl()