from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bronze-layer"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5), 
}


def insert_to_postgres(table_name: str, dataframe: pd.DataFrame):
    if dataframe is None or dataframe.empty:
        print(f"Skipping {table_name}: No data to insert.")
        return

    dataframe.columns = dataframe.columns.str.strip('\"')
    dataframe = dataframe.replace(r'\\N', np.nan, regex=True).where(pd.notna(dataframe), None)
    dataframe = dataframe.astype(object).where(pd.notna(dataframe), None)  



    hook = PostgresHook(postgres_conn_id="f1_db_conn")
    data_tuples = list(dataframe.itertuples(index=False, name=None))
    columns = list(dataframe.columns)

    print(f"🔹 Inserting {len(data_tuples)} rows into {table_name}")
    print(f"🔹 First row sample: {data_tuples[0] if data_tuples else 'No data'}")

    try:
        
        hook.insert_rows(
            table=f"f1_data_vault.{table_name}",
            rows=data_tuples,
            target_fields=columns,
            replace=False, 
        )
        
        print(f"✅ Successfully inserted {len(data_tuples)} rows into {table_name}")
    except Exception as e:
        print(f"❌ Error inserting into {table_name}: {str(e)}")



@dag(
    dag_id="silver_layer_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="ETL DAG-> Bronze to Silver Layer.",
)
def silver_layer_dag():

    @task
    def restart_db():
        pg_hook = PostgresHook(postgres_conn_id="f1_db_conn")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:

            truncate_query = """
                TRUNCATE TABLE 
                    f1_data_vault.hub_race, 
                    f1_data_vault.sat_race, 
                    f1_data_vault.hub_driver, 
                    f1_data_vault.sat_driver, 
                    f1_data_vault.hub_constructor, 
                    f1_data_vault.hub_circuit, 
                    f1_data_vault.sat_constructor, 
                    f1_data_vault.link_results, 
                    f1_data_vault.sat_results, 
                    f1_data_vault.link_qualifying, 
                    f1_data_vault.sat_qualifying, 
                    f1_data_vault.link_laps, 
                    f1_data_vault.sat_laps, 
                    f1_data_vault.link_pitstops, 
                    f1_data_vault.sat_pitstops, 
                    f1_data_vault.link_fastestlap, 
                    f1_data_vault.sat_fastestlap, 
                    f1_data_vault.hub_driverstandings, 
                    f1_data_vault.link_standings_driver, 
                    f1_data_vault.sat_driverstandings, 
                    f1_data_vault.hub_constructorstandings, 
                    f1_data_vault.link_standings_constructor, 
                    f1_data_vault.sat_constructorstandings,
                    f1_data_vault.link_result_status,
                    f1_data_vault.link_race_circuit,
                    f1_data_vault.sat_status,
                    f1_data_vault.link_practices,
                    f1_data_vault.sat_practices,
                    f1_data_vault.link_sprints,
                    f1_data_vault.sat_sprints
                CASCADE;
            """

            cursor.execute(truncate_query)
            conn.commit()
            print("✅ Successfully truncated all Data Vault tables!")

        except Exception as e:
            print(f"❌ Error during database reset: {e}")
        finally:
            cursor.close()
            conn.close()


#--------EXTRACTION TASKS-------------
    @task
    def extract_qualifying():
        PARQUET_FILE = "qualifying.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_race_circuits():
        PARQUET_FILE = "race_circuits.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_results():
        PARQUET_FILE = "results.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_laps():
        PARQUET_FILE = "laps.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_pitstops():
        PARQUET_FILE = "pitstops.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_fastest_laps():
        PARQUET_FILE = "fastest_laps.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_constructor_standings():
        PARQUET_FILE = "constructor_standings.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_driver_standings():
        PARQUET_FILE = "driver_standings.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_race_details():
        PARQUET_FILE = "race_details.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_constructors():
        PARQUET_FILE = "constructors.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_drivers():
        PARQUET_FILE = "drivers.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_status():
        PARQUET_FILE = "status.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_practices():
        PARQUET_FILE = "practices.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df

    @task
    def extract_sprints():
        PARQUET_FILE = "sprints.parquet"
        s3_path = f"s3://{BUCKET_NAME}/{PARQUET_FILE}"

        df = pd.read_parquet(s3_path, storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        })

        return df



    # Transformation Tasks (returning dictionaries) --->

    @task
    def transform_race(race_details_df:pd.DataFrame):
    #   race_details_df = extract_race_details()
        print(f"🔹 Before drop_duplicates: {race_details_df.shape[0]} rows")
        race_details_df_cleaned=race_details_df.drop_duplicates().copy()
        print(f"🔹 After drop_duplicates: {race_details_df.shape[0]} rows")
        hub_race = race_details_df_cleaned[["raceId"]]
        hub_race["raceId"] = hub_race["raceId"].apply(int)
        hub_race["loaded_at"] = pd.Timestamp.utcnow()
        sat_race = race_details_df_cleaned[['raceId', 'name_x', 'year', 'round', 'date', 'time_races', 'url_x']]
        
        return {"hub_race": hub_race, "sat_race": sat_race}

    @task
    def transform_driver(drivers_df:pd.DataFrame):

        drivers_df_cleaned=drivers_df.drop_duplicates(subset=["driverId"]).copy()
        hub_driver = drivers_df_cleaned[['driverId']]
        hub_driver["loaded_at"] = pd.Timestamp.utcnow()
        sat_driver = drivers_df_cleaned[['driverId', 'driverRef', 'number', 'code', 'forename', 'surname', 'dob', 'nationality', 'url']]
        

        return {"hub_driver": hub_driver, "sat_driver": sat_driver}

    @task
    def transform_qualifying(qualifying_df:pd.DataFrame):

        qualifying_df_cleaned=qualifying_df.drop_duplicates().copy()
        qualifying_df_cleaned.replace({'\\N': None}, inplace=True)
        qualifying_df_cleaned = qualifying_df_cleaned.dropna(axis=0, how="all")
        link_qualifying = qualifying_df_cleaned[['raceId', 'driverId', 'constructorId']]
        if "qualiId" not in link_qualifying.columns:
            link_qualifying["qualiId"] = pd.NA
        link_qualifying["qualiId"] = pd.to_numeric(link_qualifying["qualiId"], errors="coerce")
        max_id = link_qualifying['qualiId'].max()
        max_id = 0 if pd.isna(max_id) else int(max_id)
        link_qualifying.loc[pd.isna(link_qualifying["qualiId"]), "qualiId"] = range(max_id + 1, max_id + 1 + link_qualifying.shape[0])
        
        sat_qualifying = qualifying_df_cleaned[['quali_date', 'quali_time']]
        # sat_qualifying['quali_date'] = pd.to_datetime(sat_qualifying['quali_date'], errors='coerce').dt.date
        sat_qualifying.loc[:, 'quali_date'] = pd.to_datetime(sat_qualifying['quali_date'], format='%Y-%m-%d', errors='coerce').dt.date

        sat_qualifying.loc[:, 'quali_time'] = pd.to_datetime(sat_qualifying['quali_time'], format='%H:%M:%S', errors='coerce').dt.strftime('%H:%M:%S')

        link_qualifying = link_qualifying.reset_index(drop=True)
        sat_qualifying = sat_qualifying.reset_index(drop=True)
        sat_qualifying = link_qualifying[['qualiId']].merge(sat_qualifying, left_index=True, right_index=True, how="inner")
        
        
        return {"link_qualifying": link_qualifying, "sat_qualifying": sat_qualifying}

    @task
    def transform_race_circuits(race_circuits_df:pd.DataFrame):

        race_circuits_df_cleaned=race_circuits_df.drop_duplicates(subset=['circuitId']).copy()
        hub_circuit = race_circuits_df_cleaned[['circuitId']]
        hub_circuit["loaded_at"] = pd.Timestamp.utcnow()
        link_race_circuit = race_circuits_df_cleaned[['raceId', 'circuitId']]
        if "raceCircuitId" not in link_race_circuit.columns:
            link_race_circuit["raceCircuitId"] = pd.NA
        link_race_circuit["raceCircuitId"] = pd.to_numeric(link_race_circuit["raceCircuitId"], errors="coerce")
        max_id = link_race_circuit['raceCircuitId'].max()
        max_id = 0 if pd.isna(max_id) else int(max_id)
        link_race_circuit["raceCircuitId"] = range(max_id+1, max_id+1+len(link_race_circuit))
        sat_circuit = race_circuits_df_cleaned[['circuitId', 'circuitRef', 'name_y', 'location', 'country', 'lat', 'lng', 'alt', 'url_y']]

        return {"hub_circuit": hub_circuit, "link_race_circuit": link_race_circuit, "sat_circuit": sat_circuit}

    @task
    def transform_results(results_df:pd.DataFrame):

        results_df_cleaned=results_df.drop_duplicates().copy()
        link_results = results_df_cleaned[['resultId', 'raceId', 'driverId', 'constructorId','statusId']]
        sat_results = results_df_cleaned[['resultId', 'grid', 'positionText', 'positionOrder', 'points', 'laps', 'time', 'milliseconds', 'rank', 'wins', 'number_drivers']]
        

        return {"link_results": link_results, "sat_results": sat_results}

    @task
    def transform_laps(laps_df:pd.DataFrame):

        laps_df_cleaned=laps_df.drop_duplicates(subset=['raceId','driverId','lap']).copy()
        link_laps = laps_df_cleaned[['raceId', 'driverId']]
        if "lapId" not in link_laps.columns:
            link_laps["lapId"] = pd.NA
        link_laps["lapId"] = pd.to_numeric(link_laps["lapId"], errors="coerce")
        max_id = link_laps['lapId'].max()
        max_id = 0 if pd.isna(max_id) else int(max_id)
        link_laps["lapId"] = range(max_id+1, max_id+1+len(link_laps))
        
        sat_laps = laps_df_cleaned[['lap', 'position_laptimes', 'time_laptimes', 'milliseconds_laptimes']]
        link_laps = link_laps.reset_index(drop=True)

        sat_laps = sat_laps.reset_index(drop=True)
        sat_laps = link_laps[['lapId']].merge(sat_laps, left_index=True, right_index=True, how="inner")

        
        return {"link_laps": link_laps, "sat_laps": sat_laps}

    @task
    def transform_pitstops(pitstops_df:pd.DataFrame):

        pitstops_df_cleaned=pitstops_df.drop_duplicates(subset=['raceId','driverId','stop']).copy()
        pitstops_df_cleaned.replace({'\\N': None}, inplace=True)

        
        pitstops_df_cleaned.loc[:, 'time_pitstops'] = pd.to_datetime(
        pitstops_df_cleaned['time_pitstops'], format='%H:%M:%S', errors='coerce').dt.time


        pitstops_df_cleaned.loc[:, 'lap_pitstops'] = pd.to_numeric(pitstops_df_cleaned['lap_pitstops'])

        link_pitstops = pitstops_df_cleaned[['raceId', 'driverId']]
        if "pitstopId" not in link_pitstops.columns:
            link_pitstops["pitstopId"] = pd.NA
        link_pitstops["pitstopId"] = pd.to_numeric(link_pitstops["pitstopId"], errors="coerce")
        max_id = link_pitstops['pitstopId'].max()
        max_id = 0 if pd.isna(max_id) else int(max_id)
        link_pitstops["pitstopId"] = range(max_id+1, max_id+1+len(link_pitstops))
        
        sat_pitstops = pitstops_df_cleaned[['stop', 'time_pitstops', 'duration', 'milliseconds_pitstops', 'lap_pitstops']]
        link_pitstops = link_pitstops.reset_index(drop=True)
        sat_pitstops = sat_pitstops.reset_index(drop=True)
        sat_pitstops = link_pitstops[['pitstopId']].merge(sat_pitstops, left_index=True, right_index=True, how="inner")

        
        
        return {"link_pitstops": link_pitstops, "sat_pitstops": sat_pitstops}

    @task
    def transform_fastest_laps(fastest_laps_df:pd.DataFrame):

        fastest_laps_df_cleaned=fastest_laps_df.drop_duplicates().copy()
        link_fastestlap = fastest_laps_df_cleaned[['raceId', 'driverId']]
        if "fastestLapId" not in link_fastestlap.columns:
            link_fastestlap["fastestLapId"] = pd.NA
        link_fastestlap["fastestLapId"] = pd.to_numeric(link_fastestlap["fastestLapId"], errors="coerce")
        max_id = link_fastestlap['fastestLapId'].max()
        max_id = 0 if pd.isna(max_id) else int(max_id)
        link_fastestlap["fastestLapId"] = range(max_id+1, max_id+1+len(link_fastestlap))
        
        sat_fastestlap = fastest_laps_df[['fastestLap', 'fastestLapTime', 'fastestLapSpeed']]
        link_fastestlap = link_fastestlap.reset_index(drop=True)
        sat_fastestlap = sat_fastestlap.reset_index(drop=True)
        sat_fastestlap = link_fastestlap[['fastestLapId']].merge(sat_fastestlap, left_index=True, right_index=True, how="inner")
        
        
        return {"link_fastestlap": link_fastestlap, "sat_fastestlap": sat_fastestlap}

    @task
    def transform_constructor_standings(constructor_standings_df:pd.DataFrame):

        constructor_standings_df_cleaned=constructor_standings_df.drop_duplicates().copy()
        hub_constructorstandings = constructor_standings_df_cleaned[['constructorStandingsId']]
        hub_constructorstandings["loaded_at"] = pd.Timestamp.utcnow()
        link_standings_constructor = constructor_standings_df_cleaned[['constructorStandingsId', 'constructorId']]
        if "standingsConstructorId" not in link_standings_constructor.columns:
            link_standings_constructor['standingsConstructorId'] = pd.NA
        link_standings_constructor["standingsConstructorId"] = pd.to_numeric(link_standings_constructor["standingsConstructorId"], errors="coerce")
        max_id = link_standings_constructor['standingsConstructorId'].max()
        max_id = 0 if pd.isna(max_id) else int(max_id)
        link_standings_constructor["standingsConstructorId"] = range(max_id+1, max_id+1+len(link_standings_constructor))
        sat_constructorstandings = constructor_standings_df_cleaned[['wins_constructorstandings', 'points_constructorstandings', 'positionText_constructorstandings']]
        link_standings_constructor = link_standings_constructor.reset_index(drop=True)
        sat_constructorstandings = sat_constructorstandings.reset_index(drop=True)
        sat_constructorstandings = link_standings_constructor[['standingsConstructorId']].merge(sat_constructorstandings, left_index=True, right_index=True, how="inner")

        
        return {"hub_constructorstandings": hub_constructorstandings,
                "link_standings_constructor": link_standings_constructor,
                "sat_constructorstandings": sat_constructorstandings}

    @task
    def transform_driver_standings(driver_standings_df:pd.DataFrame):

        driver_standings_df_cleaned=driver_standings_df.drop_duplicates().copy()
        hub_driverstandings = driver_standings_df_cleaned[['driverStandingsId']]
        hub_driverstandings["loaded_at"] = pd.Timestamp.utcnow()
        link_standings_driver = driver_standings_df_cleaned[['driverStandingsId', 'driverId']]
        if "standingsDriverId" not in link_standings_driver.columns:
            link_standings_driver['standingsDriverId'] = pd.NA
        link_standings_driver["standingsDriverId"] = pd.to_numeric(link_standings_driver["standingsDriverId"], errors="coerce")
        max_id = link_standings_driver['standingsDriverId'].max()
        max_id = 0 if pd.isna(max_id) else int(max_id)
        link_standings_driver["standingsDriverId"] = range(max_id+1, max_id+1+len(link_standings_driver))
        
        sat_driverstandings = driver_standings_df_cleaned[['points_driverstandings', 'positionText_driverstandings', 'wins']]
        link_standings_driver = link_standings_driver.reset_index(drop=True)
        sat_driverstandings = sat_driverstandings.reset_index(drop=True)
        sat_driverstandings = link_standings_driver[['standingsDriverId']].merge(sat_driverstandings, left_index=True, right_index=True, how="inner")
        
        
        
        return {"hub_driverstandings": hub_driverstandings,
                "link_standings_driver": link_standings_driver,
                "sat_driverstandings": sat_driverstandings}

    @task
    def transform_constructors(constructors_df:pd.DataFrame):

        constructors_df_cleaned=constructors_df.drop_duplicates().copy()
        hub_constructor = constructors_df_cleaned[['constructorId']]
        hub_constructor["loaded_at"] = pd.Timestamp.utcnow()
        sat_constructor = constructors_df_cleaned[['constructorId', 'constructorRef', 'name', 'nationality_constructors', 'url_constructors']]
        
        return {"hub_constructor": hub_constructor, "sat_constructor": sat_constructor}
        
    @task
    def transform_status(status_df:pd.DataFrame):

        status_df_cleaned=status_df.drop_duplicates(subset=['statusId','resultId'])
        link_result_status = status_df_cleaned[['statusId', 'resultId']].drop_duplicates(subset=['statusId'])
        
        
        sat_status = status_df_cleaned[['statusId', 'status']].drop_duplicates()
        
        
        return {"link_result_status": link_result_status, "sat_status": sat_status}

    @task
    def transform_practices(practices_df:pd.DataFrame):

        practices_df_cleaned=practices_df.drop_duplicates().copy()
        practices_df_cleaned.replace({'\\N': None}, inplace=True)
        
        columns_to_check = ['fp1_date', 'fp1_time', 'fp2_date', 'fp2_time', 'fp3_date', 'fp3_time']
        practices_df_cleaned = practices_df_cleaned.dropna(subset=columns_to_check, how='all')

        print(f"Shape after dropping rows with all NaNs: {practices_df_cleaned.shape}")
        print(f"Missing values after cleaning:\n{practices_df_cleaned.isnull().sum()}")
        date_columns =['fp1_date', 'fp2_date', 'fp3_date']
        for col in date_columns:
            if col in practices_df_cleaned.columns:
                practices_df_cleaned[col] = practices_df_cleaned[col].replace("\\N", pd.NaT)
                practices_df_cleaned[col] = pd.to_datetime(practices_df_cleaned[col], format='%Y-%m-%d', errors='coerce').dt.strftime('%Y-%m-%d')

        time_columns = ['fp1_time', 'fp2_time', 'fp3_time']
        for col in time_columns:
            if col in practices_df_cleaned.columns:
                practices_df_cleaned[col] = practices_df_cleaned[col].replace("\\N", pd.NaT)
                practices_df_cleaned[col] = pd.to_datetime(practices_df_cleaned[col], format='%H:%M:%S', errors='coerce').dt.strftime('%H:%M:%S')

        link_practices = practices_df_cleaned[['raceId']]
        if "practiceId" not in link_practices.columns:
            link_practices['practiceId'] = pd.NA
        link_practices['practiceId'] = pd.to_numeric(link_practices['practiceId'], errors='coerce')
        max_id = link_practices["practiceId"].max()
        max_id = 0 if pd.isna(max_id) else int(max_id)
        link_practices['practiceId'] = range(max_id+1, max_id+1+len(link_practices))
        
        sat_practices = practices_df_cleaned[['fp1_date', 'fp1_time', 'fp2_date', 'fp2_time', 'fp3_date', 'fp3_time']]
        link_practices = link_practices.reset_index(drop=True)
        sat_practices = sat_practices.reset_index(drop=True)
        sat_practices = link_practices[['practiceId']].merge(sat_practices, left_index=True, right_index=True, how="inner")
        
        
        return {"link_practices": link_practices, "sat_practices": sat_practices}

    @task
    def transform_sprints(sprints_df:pd.DataFrame):

        sprints_df_cleaned=sprints_df.drop_duplicates().copy()
        sprints_df_cleaned.replace({'\\N': None}, inplace=True)
        columns_to_check = ['sprint_date','sprint_time']
        sprints_df_cleaned = sprints_df_cleaned.dropna(subset=columns_to_check, how='all')


        sprints_df_cleaned['sprint_date'] = sprints_df_cleaned['sprint_date'].replace("\\N", pd.NaT)
        sprints_df_cleaned['sprint_date']= pd.to_datetime(sprints_df_cleaned['sprint_date'], format='%Y-%m-%d', errors='coerce').dt.strftime('%Y-%m-%d')

        sprints_df_cleaned['sprint_time']= sprints_df_cleaned['sprint_time'].replace("\\N", pd.NaT)
        sprints_df_cleaned.loc[:, 'sprint_time'] = pd.to_datetime(sprints_df_cleaned['sprint_time'], format='%H:%M:%S', errors='coerce').dt.time 



        link_sprints = sprints_df_cleaned[['raceId', 'driverId', 'constructorId']]
        if "sprintId" not in link_sprints.columns:
            link_sprints['sprintId'] = pd.NA
        link_sprints['sprintId'] = pd.to_numeric(link_sprints['sprintId'], errors='coerce')
        max_id = link_sprints["sprintId"].max()
        max_id = 0 if pd.isna(max_id) else int(max_id)
        link_sprints['sprintId'] = range(max_id+1, max_id+1+len(link_sprints))
        
        sat_sprints = sprints_df_cleaned[['sprint_date', 'sprint_time']]
        link_sprints = link_sprints.reset_index(drop=True)
        sat_sprints = sat_sprints.reset_index(drop=True)
        sat_sprints = link_sprints[['sprintId']].merge(sat_sprints, left_index=True, right_index=True, how="inner")
        
        
        return {"link_sprints": link_sprints, "sat_sprints": sat_sprints}




    # ---------------------- LOAD HUB TABLES ----------------------
    @task
    def load_hub_tables(race_dict, driver_dict, constructors_dict, constructor_standings_dict, driver_standings_dict,race_circuits_dict):
        insert_to_postgres("hub_race", race_dict["hub_race"])
        insert_to_postgres("hub_driver", driver_dict["hub_driver"])
        insert_to_postgres("hub_constructor", constructors_dict["hub_constructor"])
        insert_to_postgres("hub_constructorstandings", constructor_standings_dict["hub_constructorstandings"])
        insert_to_postgres("hub_driverstandings", driver_standings_dict["hub_driverstandings"])
        insert_to_postgres("hub_circuit", race_circuits_dict["hub_circuit"])

    # ---------------------- LOAD LINK TABLES ----------------------
    @task
    def load_link_tables(qualifying_dict, race_circuits_dict, results_dict, laps_dict, pitstops_dict, fastest_laps_dict, 
                        constructor_standings_dict, driver_standings_dict, status_dict, practices_dict, sprints_dict):
        insert_to_postgres("link_qualifying", qualifying_dict["link_qualifying"])
        insert_to_postgres("link_race_circuit", race_circuits_dict["link_race_circuit"])
        insert_to_postgres("link_results", results_dict["link_results"])
        insert_to_postgres("link_laps", laps_dict["link_laps"])
        insert_to_postgres("link_pitstops", pitstops_dict["link_pitstops"])
        insert_to_postgres("link_fastestlap", fastest_laps_dict["link_fastestlap"])
        insert_to_postgres("link_standings_constructor", constructor_standings_dict["link_standings_constructor"])
        insert_to_postgres("link_standings_driver", driver_standings_dict["link_standings_driver"])
        insert_to_postgres("link_result_status", status_dict["link_result_status"])
        insert_to_postgres("link_practices", practices_dict["link_practices"])
        insert_to_postgres("link_sprints", sprints_dict["link_sprints"])

    # ---------------------- LOAD SATELLITE TABLES ----------------------
    @task
    def load_satellite_tables(race_dict, driver_dict, constructors_dict, qualifying_dict, race_circuits_dict, results_dict, 
                            laps_dict, pitstops_dict, fastest_laps_dict, constructor_standings_dict, driver_standings_dict,
                            status_dict, practices_dict, sprints_dict):
        insert_to_postgres("sat_race", race_dict["sat_race"])
        insert_to_postgres("sat_driver", driver_dict["sat_driver"])
        insert_to_postgres("sat_constructor", constructors_dict["sat_constructor"])
        insert_to_postgres("sat_qualifying", qualifying_dict["sat_qualifying"])
        insert_to_postgres("sat_circuit", race_circuits_dict["sat_circuit"])
        insert_to_postgres("sat_results", results_dict["sat_results"])
        insert_to_postgres("sat_laps", laps_dict["sat_laps"])
        insert_to_postgres("sat_pitstops", pitstops_dict["sat_pitstops"])
        insert_to_postgres("sat_fastestlap", fastest_laps_dict["sat_fastestlap"])
        insert_to_postgres("sat_constructorstandings", constructor_standings_dict["sat_constructorstandings"])
        insert_to_postgres("sat_driverstandings", driver_standings_dict["sat_driverstandings"])
        insert_to_postgres("sat_status", status_dict["sat_status"])
        insert_to_postgres("sat_practices", practices_dict["sat_practices"])
        insert_to_postgres("sat_sprints", sprints_dict["sat_sprints"])

    # Restart DB first
    db_reset = restart_db()

    # Extract Data
    race_details_df = extract_race_details()
    drivers_df = extract_drivers()
    constructors_df = extract_constructors()
    race_circuits_df = extract_race_circuits()
    results_df = extract_results()
    qualifying_df = extract_qualifying()
    laps_df = extract_laps()
    pitstops_df = extract_pitstops()
    fastest_laps_df = extract_fastest_laps()
    constructor_standings_df = extract_constructor_standings()
    driver_standings_df = extract_driver_standings()
    status_df = extract_status()
    practices_df = extract_practices()
    sprints_df = extract_sprints()

    # Transform Data
    race_data = transform_race(race_details_df)
    driver_data = transform_driver(drivers_df)
    constructors_data = transform_constructors(constructors_df)
    race_circuits_data = transform_race_circuits(race_circuits_df)
    results_data = transform_results(results_df)
    qualifying_data = transform_qualifying(qualifying_df)
    laps_data = transform_laps(laps_df)
    pitstops_data = transform_pitstops(pitstops_df)
    fastest_laps_data = transform_fastest_laps(fastest_laps_df)
    constructor_standings_data = transform_constructor_standings(constructor_standings_df)
    driver_standings_data = transform_driver_standings(driver_standings_df)
    status_data = transform_status(status_df)
    practices_data = transform_practices(practices_df)
    sprints_data = transform_sprints(sprints_df)

    # Load Data in Proper Order (HUBS → LINKS → SATELLITES)
    load_hubs = load_hub_tables(race_data, driver_data, constructors_data, constructor_standings_data, driver_standings_data, race_circuits_data)
    load_links = load_link_tables(qualifying_data, race_circuits_data,results_data, laps_data, pitstops_data, fastest_laps_data,
                                constructor_standings_data, driver_standings_data,  status_data, practices_data, sprints_data)
    load_sats = load_satellite_tables(race_data, driver_data, constructors_data, qualifying_data, race_circuits_data, results_data,
                                    laps_data, pitstops_data, fastest_laps_data, constructor_standings_data, driver_standings_data,
                                    status_data, practices_data, sprints_data)

    # -------- DEPENDENCIES ----------
    db_reset >> [
    race_details_df, drivers_df, constructors_df, race_circuits_df, results_df,
    qualifying_df, laps_df, pitstops_df, fastest_laps_df, constructor_standings_df,
    driver_standings_df, status_df, practices_df, sprints_df
    ]

    race_details_df >> race_data >>load_hubs
    drivers_df >> driver_data >> load_hubs
    constructors_df >> constructors_data >> load_hubs
    race_circuits_df >> race_circuits_data >> load_hubs
    results_df >> results_data >> load_hubs
    qualifying_df >> qualifying_data >> load_hubs
    laps_df >> laps_data >> load_hubs
    pitstops_df >> pitstops_data >> load_hubs
    fastest_laps_df >> fastest_laps_data >> load_hubs
    constructor_standings_df >> constructor_standings_data >> load_hubs
    driver_standings_df >> driver_standings_data >> load_hubs
    status_df >> status_data >> load_hubs
    practices_df >> practices_data >> load_hubs
    sprints_df >> sprints_data >> load_hubs

    load_hubs >> load_links
    load_links >> load_sats

silver_layer_dag()