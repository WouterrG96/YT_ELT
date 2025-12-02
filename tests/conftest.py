import os
import pytest
import psycopg2
from unittest import mock
from airflow.models import Variable, Connection, DagBag


@pytest.fixture
def api_key():
    # Mock Airflow Variable via env var (AIRFLOW_VAR_<NAME>) and return its value
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="FAKE_KEY1234"):
        yield Variable.get("API_KEY")


@pytest.fixture
def channel_handle():
    # Mock Airflow Variable via env var (AIRFLOW_VAR_<NAME>) and return its value
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="FAKENAME"):
        yield Variable.get("CHANNEL_HANDLE")


@pytest.fixture
def mock_postgres_conn_vars():
    # Build a mock Airflow Connection and expose it via AIRFLOW_CONN_* for secret lookup
    conn = Connection(
        login="mock_username",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db_name",  # schema is the db name
    )
    conn_uri = conn.get_uri()

    # Airflow reads connections from env vars in the AIRFLOW_CONN_<CONN_ID> format
    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")


@pytest.fixture()
def dagbag():
    # Load DAGs from the configured dags_folder for integrity tests
    yield DagBag()


@pytest.fixture()
def airflow_variable():
    # Helper to read AIRFLOW_VAR_* directly from the environment (no Airflow Variable API)
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)

    return get_airflow_variable


@pytest.fixture
def real_postgres_connection():
    # Connect to a real Postgres instance using env vars (integration test fixture)
    dbname = os.getenv("ELT_DATABASE_NAME")
    user = os.getenv("ELT_DATABASE_USERNAME")
    password = os.getenv("ELT_DATABASE_PASSWORD")
    host = os.getenv("POSTGRES_CONN_HOST")
    port = os.getenv("POSTGRES_CONN_PORT")

    conn = None

    try:
        # Open DB connection and yield it to the test
        conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        yield conn

    except psycopg2.Error as e:
        # Fail the test immediately if the DB is unreachable
        pytest.fail(f"Failed to connect to the database: {e}")

    finally:
        # Always close the connection to avoid leaking sessions
        if conn:
            conn.close()
