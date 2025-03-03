from airflow.decorators import dag, task
import datetime
import pandas as pd
import io
from minio import Minio

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bronze-layer"
CSV_FILE_PATH= "/opt/airflow/data/dataEngineeringDataset.csv"

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

        df = pd.read_csv(CSV_FILE_PATH, low_memory=False)

        
        merged_data = {
            "qualifying" : df[["quali_date","quali_time","raceId","driverId","constructorId"]],
            "race_circuits" : df[["raceId","circuitId","circuitRef","name_y","location","country","lat","lng","alt","url_y"]],
            "results": df[["resultId","raceId","driverId","constructorId","statusId","grid","positionText","positionOrder",
                           "points","laps","time","milliseconds","rank","wins","number_drivers"]],
            "laps" :df[["raceId","driverId","lap","position_laptimes","time_laptimes","milliseconds_laptimes"]],
            "pitstops" :df[["raceId","driverId","stop","time_pitstops","duration","milliseconds_pitstops","lap_pitstops"]],
            "fastest_laps" : df[["raceId","driverId","fastestLap","fastestLapTime","fastestLapSpeed"]],
            "constructor_standings" : df[["constructorStandingsId","constructorId","wins_constructorstandings",
                            "points_constructorstandings","positionText_constructorstandings"]],
            "driver_standings" : df[["driverStandingsId","driverId","points_driverstandings","positionText_driverstandings","wins"]],
            "race_details" : df[["raceId","name_x","year","round","date","time_races","url_x"]],
            "constructors" : df[["constructorId","constructorRef","name","nationality_constructors","url_constructors"]],
            "drivers" : df[["driverId","driverRef","number","code","forename","surname","dob","nationality","url"]],
            "status" : df[["statusId","resultId","status"]],
            "practices" : df[["raceId","fp1_date","fp1_time","fp2_date","fp2_time","fp3_date","fp3_time"]],
            "sprints" : df[["raceId","driverId","constructorId","sprint_date","sprint_time"]],
        }
        

        for category, category_df in merged_data.items():
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


