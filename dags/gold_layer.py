from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 20),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
   dag_id="gold_layer_dag",
   default_args=default_args,
   schedule_interval="@daily",
   catchup=False,
   template_searchpath="/opt/airflow/dags/sql",
   description="Transforms Data Vault (Silver) to Star Schema (Gold) using SQL operators.",
   tags=["medallion_schema", "gold"]
)
def gold_layer_dag():
    

    @task
    def restart_db():
        pg_hook = PostgresHook(postgres_conn_id="f1_db_conn")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
    
        try:
            truncate_query = """
                TRUNCATE TABLE 
                    gold.Dim_Circuit, 
                    gold.Dim_Race, 
                    gold.Dim_Driver, 
                    gold.Dim_Constructor, 
                    gold.Dim_Status, 
                    gold.Dim_DriverStandings, 
                    gold.Dim_ConstructorStandings, 
                    gold.Dim_Qualifying, 
                    gold.Dim_Practice, 
                    gold.Dim_Laps, 
                    gold.Dim_Sprint, 
                    gold.Dim_Pit_Stops, 
                    gold.Fact_Race_Results
                RESTART IDENTITY CASCADE;
            """
        
            cursor.execute(truncate_query)
            conn.commit()
            print("✅ Successfully truncated all Star Schema tables!")

        except Exception as e:
            print(f"❌ Error during database reset: {e}")
        finally:
            cursor.close()
            conn.close()

        return "Database Reset Complete"

    with open("/opt/airflow/dags/sql/transform_dim_circuit.sql", "r") as file:
        sql_circuit_query = file.read()

    transform_dim_circuit = SQLExecuteQueryOperator(
        task_id="transform_dim_circuit",
        conn_id="f1_db_conn",
        sql=sql_circuit_query
    )

    with open("/opt/airflow/dags/sql/transform_dim_race.sql", "r") as file:
        sql_race_query = file.read()

    transform_dim_race = SQLExecuteQueryOperator(
        task_id="transform_dim_race",
        conn_id="f1_db_conn",
        sql=sql_race_query
    )

    with open("/opt/airflow/dags/sql/transform_dim_driver.sql", "r") as file:
        sql_driver_query = file.read()

    transform_dim_driver = SQLExecuteQueryOperator(
        task_id="transform_dim_driver",
        conn_id="f1_db_conn",
        sql=sql_driver_query
    )

    with open("/opt/airflow/dags/sql/transform_dim_status.sql", "r") as file:
        sql_status_query = file.read()

    transform_dim_status = SQLExecuteQueryOperator(
        task_id="transform_dim_status",
        conn_id="f1_db_conn",
        sql=sql_status_query
    )

    with open("/opt/airflow/dags/sql/transform_dim_constructor.sql", "r") as file:
        sql_constructor_query = file.read()

    transform_dim_constructor = SQLExecuteQueryOperator(
        task_id="transform_dim_constructor",
        conn_id="f1_db_conn",
        sql=sql_constructor_query
    )

    with open("/opt/airflow/dags/sql/transform_dim_driverstandings.sql", "r") as file:
        sql_driverstandings_query = file.read()

    transform_dim_driverstandings= SQLExecuteQueryOperator(
        task_id="transform_dim_driverstandings",
        conn_id="f1_db_conn",
        sql=sql_driverstandings_query
    )

    with open("/opt/airflow/dags/sql/transform_dim_constructorstandings.sql", "r") as file:
        sql_constructorstandings_query = file.read()

    transform_dim_constructorstandings= SQLExecuteQueryOperator(
        task_id="transform_dim_constructorstandings",
        conn_id="f1_db_conn",
        sql=sql_constructorstandings_query
    )

    with open("/opt/airflow/dags/sql/transform_dim_qualifying.sql", "r") as file:
        sql_qualifying_query = file.read()

    transform_dim_qualifying = SQLExecuteQueryOperator(
        task_id="transform_dim_qualifying",
        conn_id="f1_db_conn",
        sql=sql_qualifying_query
    )

    with open("/opt/airflow/dags/sql/transform_dim_practice.sql", "r") as file:
        sql_practice_query = file.read()

    transform_dim_practice = SQLExecuteQueryOperator(
        task_id="transform_dim_practice",
        conn_id="f1_db_conn",
        sql=sql_practice_query
    )

    with open("/opt/airflow/dags/sql/transform_dim_laps.sql", "r") as file:
        sql_laps_query = file.read()

    transform_dim_laps = SQLExecuteQueryOperator(
        task_id="transform_dim_laps",
        conn_id="f1_db_conn",
        sql=sql_laps_query
    )

    with open("/opt/airflow/dags/sql/transform_dim_sprint.sql", "r") as file:
        sql_sprint_query = file.read()

    transform_dim_sprint= SQLExecuteQueryOperator(
        task_id="transform_dim_sprint",
        conn_id="f1_db_conn",
        sql=sql_sprint_query
    )

    with open("/opt/airflow/dags/sql/transform_dim_pitstops.sql", "r") as file:
        sql_pitstops_query = file.read()

    transform_dim_pit_stops= SQLExecuteQueryOperator(
        task_id="transform_dim_pit_stops",
        conn_id="f1_db_conn",
        sql=sql_pitstops_query
    )

    with open("/opt/airflow/dags/sql/transform_fact_race_result.sql", "r") as file:
        sql_fact_query = file.read()

    transform_fact_race_results = SQLExecuteQueryOperator(
        task_id="transform_fact_race_results",
        conn_id="f1_db_conn",
        sql=sql_fact_query
    )


    db_reset=restart_db()
    transform_dim_circuit >> transform_dim_race
    transform_dim_driver
    transform_dim_constructor
    transform_dim_status
    

    transform_dim_driver >> transform_dim_driverstandings
    transform_dim_constructor >> transform_dim_constructorstandings
    

    transform_dim_race >> transform_dim_qualifying
    transform_dim_race >> transform_dim_practice
    transform_dim_race >> transform_dim_laps
    transform_dim_race >> transform_dim_sprint
    transform_dim_race >> transform_dim_pit_stops
    
    transform_dim_driver >> transform_dim_laps
    transform_dim_driver >> transform_dim_sprint
    transform_dim_driver >> transform_dim_pit_stops

    db_reset >> transform_dim_circuit
    db_reset>>[
        transform_dim_circuit,
        transform_dim_race, 
        transform_dim_driver, 
        transform_dim_constructor, 
        transform_dim_status, 
        transform_dim_qualifying, 
        transform_dim_practice, 
        transform_dim_laps, 
        transform_dim_sprint, 
        transform_dim_pit_stops, 
        transform_dim_driverstandings, 
        transform_dim_status,
        transform_dim_constructorstandings
    ] >> transform_fact_race_results

gold_layer_dag()