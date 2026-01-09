import os
import pytest
from unittest import mock # Mocking Airflow Variable for tests
from airflow.models import Variable, Connection, DagBag
import psycopg2

@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY") # Variable.get fetches the value of any Airflow Variable

@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="MRSOFT"):
        yield Variable.get("CHANNEL_HANDLE") 

@pytest.fixture
def mock_postgres_connection():
    conn = Connection(
        login = "mock_username",
        password = "mock_password",
        host = "mock_host",
        port=1234,
        schema="mock_db_name" # Schema is the db name
    )
    conn_uri = conn.get_uri() # Method to create connection URI for postgres

    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT") # Get connection from secrets to fetch the connection details stored in Airflow secrets backend

@pytest.fixture
def dagbag():
    yield DagBag() # DagBag loads all the DAGs defined in the Airflow dags 


# For integration test

# Fixture to get Airflow Variable values from environment variables (real credentials)
@pytest.fixture
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)
    
    return get_airflow_variable


@pytest.fixture
def real_postgres_connection(): # Fixture to establish real connection to Postgres using env variables (real credentials)
    dbname = os.getenv("ELT_DATABASE_NAME")
    user = os.getenv("ELT_DATABASE_USERNAME")
    password = os.getenv("ELT_DATABASE_PASSWORD")
    host = os.getenv("POSTGRES_CONN_HOST")
    port = os.getenv("POSTGRES_CONN_PORT")

    conn = None

    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

        yield conn # Yield the connection for use in tests

    except psycopg2.Error as e:
        pytest.fail(f"Failed to connect to the database: {e}")

    finally:
        if conn:
            conn.close()