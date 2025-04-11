from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime
import pandas as pd
import logging
import io

default_args = {
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
}

PROJECT_ID = 'videogames-456403'
BUCKET_NAME = 'vg-data'
DATASET = 'vg_raw'
GCP_CONN_ID = 'google_cloud_default'

def validate_gcs_csv(file_path: str, expected_columns: list[str], numeric_columns: list[str] = []):
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    logging.info(f"Validating file: {file_path}")

    file_bytes = hook.download(bucket_name="vg-data", object_name=file_path)
    df = pd.read_csv(io.BytesIO(file_bytes))

    # Log missing columns 
    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        logging.warning(f"File {file_path} is missing columns: {missing_cols}")

    # Drop duplicates
    before = len(df)
    df.drop_duplicates(inplace=True)
    after = len(df)
    if before != after:
        logging.info(f"Removed {before - after} duplicate rows from {file_path}")

    # Log nulls
    for col in expected_columns:
        if col in df.columns:
            nulls = df[col].isnull().sum()
            if nulls > 0:
                logging.warning(f"Column '{col}' has {nulls} null values in {file_path}")

    # Check types if present
    for col in numeric_columns:
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            logging.warning(f"Column '{col}' in {file_path} is not numeric — actual type: {df[col].dtype}")

    # Row count sanity
    if len(df) < 10:
        logging.warning(f"File {file_path} has very few rows: {len(df)}")

    logging.info(f"Validation completed for {file_path}")


with DAG('load_validate_gcs_to_bq',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         description='Validate and load all gaming CSVs from GCS to BigQuery') as dag:

    files = [
        # PlayStation files
        {
            'platform': 'playstation',
            'file': 'achievements.csv',
            'table': 'ps_achievements_raw',
            'expected_columns': ['achievementid', 'gameid', 'title', 'description', 'points'],
            'numeric_columns': ['points']
        },
        {
            'platform': 'playstation',
            'file': 'games.csv',
            'table': 'ps_games_raw',
            'expected_columns': ['gameid', 'title', 'developers', 'publishers', 'genres', 'supported_languages', 'release_date'],
            'numeric_columns': ['gameid']
        },
        {
            'platform': 'playstation',
            'file': 'history.csv',
            'table': 'ps_history_raw',
            'expected_columns': ['playerid', 'gameid', 'timestamp'],
            'numeric_columns': ['playerid', 'gameid']
        },
        {
            'platform': 'playstation',
            'file': 'players.csv',
            'table': 'ps_players_raw',
            'expected_columns': ['playerid'],
            'numeric_columns': ['playerid']
        },
        {
            'platform': 'playstation',
            'file': 'prices.csv',
            'table': 'ps_prices_raw',
            'expected_columns': ['gameid', 'usd', 'eur', 'gbp', 'jpy', 'rub', 'date_acquired'],
            'numeric_columns': ['gameid']
        },
        {
            'platform': 'playstation',
            'file': 'purchased_games.csv',
            'table': 'ps_purchased_games_raw',
            'expected_columns': ['playerid', 'library'],
            'numeric_columns': []
        },
        # Xbox files
        {
            'platform': 'xbox',
            'file': 'achievements.csv',
            'table': 'xbox_achievements_raw',
            'expected_columns': ['achievementid', 'gameid', 'title', 'description', 'points'],
            'numeric_columns': ['points']
        },
        {
            'platform': 'xbox',
            'file': 'games.csv',
            'table': 'xbox_games_raw',
            'expected_columns': ['gameid', 'title', 'developers', 'publishers', 'genres', 'supported_languages', 'release_date'],
            'numeric_columns': ['gameid']
        },
        {
            'platform': 'xbox',
            'file': 'history.csv',
            'table': 'xbox_history_raw',
            'expected_columns': ['playerid', 'gameid', 'timestamp'],
            'numeric_columns': ['playerid', 'gameid']
        },
        {
            'platform': 'xbox',
            'file': 'players.csv',
            'table': 'xbox_players_raw',
            'expected_columns': ['playerid'],
            'numeric_columns': ['playerid']
        },
        {
            'platform': 'xbox',
            'file': 'prices.csv',
            'table': 'xbox_prices_raw',
            'expected_columns': ['gameid', 'usd', 'eur', 'gbp', 'jpy', 'rub', 'date_acquired'],
            'numeric_columns': ['gameid']
        },
        {
            'platform': 'xbox',
            'file': 'purchased_games.csv',
            'table': 'xbox_purchased_games_raw',
            'expected_columns': ['playerid', 'library'],
            'numeric_columns': []
        }
    ]

    for f in files:
        file_path = f"{f['platform']}/{f['file']}"
        table_id = f"{PROJECT_ID}.{DATASET}.{f['table']}"

        validate = PythonOperator(
            task_id=f"validate_{f['platform']}_{f['file'].replace('.csv', '')}",
            python_callable=validate_gcs_csv,
            op_kwargs={
                'file_path': file_path,
                'expected_columns': f['expected_columns'],
                'numeric_columns': f['numeric_columns']
            }
        )

        load = GCSToBigQueryOperator(
            task_id=f"load_{f['platform']}_{f['file'].replace('.csv', '')}",
            bucket=BUCKET_NAME,
            source_objects=[file_path],
            destination_project_dataset_table=table_id,
            skip_leading_rows=1,
            source_format='CSV',
            autodetect=True,
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=GCP_CONN_ID,
            ignore_unknown_values=True,    
            allow_quoted_newlines=True 
        )

        validate >> load

