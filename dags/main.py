from airflow import DAG
import pendulum # for timezone handling
from datetime import timedelta, datetime # for defining default args and scheduling
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

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

with DAG(
    dag_id="produce_json",
    default_args= default_args,
    description="DAG to extract YouTube video stats and save raw data to JSON",
    schedule= "0 14 * * *", # Run every day at 2PM
    catchup=False # Don't catch up on past runs
) as dag:
    
    # Define tasks (using TaskFlow API)
    playlist_id = get_playlist_id.override(task_id="fetch_playlist_id")() # Example on how to override task_id
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_json = save_to_json(extract_data)

    # Define dependencies. With TaskFlow API is optional if using the return values as inputs.
    playlist_id >> video_ids >> extract_data >> save_json