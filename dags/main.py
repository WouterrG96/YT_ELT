from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from api.video_stats import (
    get_playlist_id,
    get_video_ids,
    extract_video_data,
    save_to_json,
)

from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

# Define the local timezone
local_tz = pendulum.timezone("Europe/Malta")

# Default Args
default_args = {
    "owner": "wouterrg96",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.net",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

# Variables
staging_schema = "staging"
core_schema = "core"

# DAG 1: produce_json
with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to produce JSON file with raw data',
    schedule=None,
    # Don't backfill historical runs between start_date and today
    catchup=False
) as dag:

    # --- Define tasks (TaskFlow API / operators returned by these functions) ---
    # Task 1: Resolve the playlist/channel identifier
    playlist_id = get_playlist_id()

    # Task 2: Fetch all video IDs from that playlist
    video_ids = get_video_ids(playlist_id)

    # Task 3: Extract stats/metadata for all fetched video IDs
    extract_data = extract_video_data(video_ids)

    # Task 4: Persist extracted raw data to a JSON file (landing layer)
    save_to_json_task = save_to_json(extract_data)

    # Task 5: Trigger the update db DAG after data has been extracted
    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    # --- Define dependencies (execution order) ---
    # Ensures extraction happens step-by-step, and JSON is written last
    playlist_id >> video_ids >> extract_data >> save_to_json_task


# DAG 2: update_db
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    catchup=False,
    # No schedule => this DAG is triggered manually or by another DAG operator
    schedule=None,
) as dag_update:

    # --- Define tasks ---
    # Task 1: Load JSON and upsert into staging schema/table (raw-ish snapshot)
    update_staging = staging_table()

    # Task 2: Read staging, transform, then upsert into core schema/table (curated model)
    update_core = core_table()

    # Task 3: Trigger the data quality DAG after DB updates complete
    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )

    # --- Define dependencies ---
    # Ensure staging update happens before core, and data quality runs last
    update_staging >> update_core >> trigger_data_quality


# DAG 3: data_quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both layers in the database",
    catchup=False,
    # No schedule => this DAG is triggered manually or by another DAG/operator
    schedule=None,
) as dag_quality:

    # --- Define tasks ---
    # Task 1: Run Soda checks against the staging schema
    soda_validate_staging = yt_elt_data_quality(staging_schema)

    # Task 2: Run Soda checks against the core schema
    soda_validate_core = yt_elt_data_quality(core_schema)

    # --- Define dependencies ---
    # Ensure staging checks run before core checks
    soda_validate_staging >> soda_validate_core