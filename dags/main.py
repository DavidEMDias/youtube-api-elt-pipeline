from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum # for timezone handling
from datetime import timedelta, datetime # for defining default args and scheduling
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

# Define the local timezone
local_tz = pendulum.timezone("Europe/Lisbon")

# Default Args
default_args = {
    "owner": "david",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "david@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

STAGING_SCHEMA = "staging"
CORE_SCHEMA = "core"

# Dag 1 - Produce JSON
with DAG(
    dag_id="produce_json",
    default_args= default_args,
    description="DAG to extract YouTube video stats and save raw data to JSON",
    schedule= "0 14 * * *", # Run every day at 2PM
    catchup=False # Don't catch up on past runs
) as dag_produce:
    
    # Define tasks (using TaskFlow API)
    playlist_id = get_playlist_id.override(task_id="fetch_playlist_id")() # Example on how to override task_id
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_json = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    # Define dependencies. With TaskFlow API is optional if using the return values as inputs.
    playlist_id >> video_ids >> extract_data >> save_json >> trigger_update_db



# Dag 2 - Update Datawarehouse
with DAG(
    dag_id="update_db",
    default_args= default_args,
    description="DAG to process JSON File and insert data into staging and core layer",
    schedule=None,
    catchup=False # Don't catch up on past runs
) as dag_update:
    
    # Define tasks (using TaskFlow API)
    update_staging = staging_table.override(task_id="update_staging_layer")() # Example on how to override task_id
    update_core = core_table.override(task_id="update_core_layer")() # For override method to work function must be TaskFlow-decorated function

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )

    # Define dependencies. With TaskFlow API is optional if using the return values as inputs.
    update_staging >> update_core >> trigger_data_quality



# Dag 3 - Data Quality Checks
with DAG(
    dag_id="data_quality",
    default_args= default_args,
    description="DAG to run data quality checks on the datawarehouse using Soda - both layers in the dwh",
    schedule=None,
    catchup=False
) as dag_quality:
    
    # Define tasks (using BashOperator inside function)
    validate_staging = yt_elt_data_quality(STAGING_SCHEMA) #Task_id will be set inside the function
    validate_core = yt_elt_data_quality(CORE_SCHEMA) 

    # Define dependencies
    validate_staging >> validate_core