from airflow.decorators import dag, task
import datetime
import pandas as pd
import io
from minio import Minio

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bronze-layer"

@dag(
    schedule=None,
    start_date=datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["bronze", "medallion_schema"],
)
def bronze_layer_dag():
    
    @task()
    def extract_and_save_as_parquet():
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )

        df = pd.read_csv("/opt/airflow/data/dataEngineeringDataset.csv", low_memory=False)

        groupings = {
            "driver": df[[
                "driverId", "driverRef", "number", "code", "forename", "surname", "dob", "nationality", "url"
            ]],
            "raceResult": df[[
                "resultId", "raceId", "driverId", "constructorId", "grid", "positionOrder", "points", "laps",
                "time", "milliseconds", "fastestLap", "fastestLapTime", "fastestLapSpeed", "rank",
                "positionText", "wins", "number_drivers", "statusId", "driverStandingsId", "constructorStandingsId"
            ]],
            "race": df[[
                "raceId", "year", "round", "circuitId", "name_x", "date", "time_races", "url_x"
            ]],
            "circuit": df[[
                "circuitId", "circuitRef", "name_y", "location", "country", "lat", "lng", "alt", "url_y"
            ]],
            "constructor": df[[
                "constructorId", "constructorRef", "name", "nationality_constructors", "url_constructors"
            ]],
            "status": df[[
                "statusId", "status"
            ]],
            "qualifying": df[[
                "raceId", "driverId", "quali_date", "quali_time"
            ]],
            "sprint": df[[
                "raceId", "driverId", "sprint_date", "sprint_time"
            ]],
            "practice": df[[
                "raceId", "fp1_date", "fp1_time", "fp2_date", "fp2_time", "fp3_date", "fp3_time"
            ]],
            "pitStops": df[[
                "raceId", "driverId", "stop", "lap_pitstops", "time_pitstops", "duration", "milliseconds_pitstops"
            ]],
            "driverStandings": df[[
                "driverStandingsId", "points_driverstandings", "positionText_driverstandings", "wins"
            ]],
            "constructorStandings": df[[
                "constructorStandingsId", "points_constructorstandings", "positionText_constructorstandings",
                "wins_constructorstandings", "constructorId"
            ]],
            "laps": df[[
                "raceId", "driverId", "lap", "position_laptimes", "time_laptimes", "milliseconds_laptimes"
            ]],
            "fastestLaps": df[[ 'raceId','driverId',"fastestLap", "fastestLapTime", "fastestLapSpeed"
            ]]
        }

        for category, category_df in groupings.items():
            parquet_buffer = io.BytesIO()
            category_df.to_parquet(parquet_buffer, engine="fastparquet")
            parquet_buffer.seek(0)

            object_name = f"{category}.parquet"
            minio_client.put_object(
                BUCKET_NAME, object_name, parquet_buffer, len(parquet_buffer.getvalue()),
                content_type="application/octet-stream"
            )

            print(f"✅ Uploaded {object_name} to MinIO Bronze Layer.")

    extract_and_save_as_parquet()

bronze_layer_dag()
