from airflow.decorators import dag, task
import datetime
import pandas as pd

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bronze-layer"


@dag(
    schedule=None,
    start_date=datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["silver", "medallion_schema"],
)
def silver_layer_dag():
    
    @task()
    def process_race_data():
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        print("✅ Data Ingested from MinIO:")
        print(df.head())

        hub_race = df[['raceId']].drop_duplicates()
        hub_race["loaded_at"] = pd.Timestamp.utcnow()

        sat_race = df[['raceId', 'name_x', 'year', 'round', 'date', 'time_races', 'url_x']].drop_duplicates()
        
        sat_race["date"] = pd.to_datetime(sat_race["date"])
        sat_race["time_races"] = pd.to_datetime(sat_race["time_races"], format="%H:%M:%S", errors="coerce").dt.time

        print("✅ Transformed Hub_Race Table:")
        print(hub_race.head())

        print("✅ Transformed Sat_Race Table:")
        print(sat_race.head())

        return hub_race, sat_race

    process_race_data()

silver_layer_dag()
