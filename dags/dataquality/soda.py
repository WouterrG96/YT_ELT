import logging
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

SODA_PATH = "/opt/airflow/include/soda"   # Soda configs/checks location
DATASOURCE = "pg_datasource"              # Soda datasource name


def yt_elt_data_quality(schema):
    """Return a BashOperator that runs `soda scan` for the given schema."""
    try:
        return BashOperator(
            task_id=f"soda_test_{schema}",  # unique per schema
            bash_command=(
                f"soda scan -d {DATASOURCE} "
                f"-c {SODA_PATH}/configuration.yml "
                f"-v SCHEMA={schema} "
                f"{SODA_PATH}/checks.yml"
            ),
        )
    except Exception as e:
        logger.error(f"Failed Soda scan task for schema={schema}")
        raise
