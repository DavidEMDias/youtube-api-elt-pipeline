def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234" # Assertion to verify the mocked API key value is really what we defined in the fixture


def test_channel_handle(channel_handle):
    assert channel_handle == "MRSOFT" 

def test_postgres_conn(mock_postgres_connection): # mock_postgres_connection is the fixture defined in conftest.py
    conn = mock_postgres_connection # Just for simplicity purposes

    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"


def test_dags_integrity(dagbag):
    
    # 1. Import errors check
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}." # Print object to help debugging
    print("-----------")
    print("Import Errors:")
    print(dagbag.import_errors)


    # 2. All expected DAGS are loaded
    expected_dag_ids = ["produce_json", "update_db", "data_quality"]
    loaded_dag_ids = list(dagbag.dags.keys()) 

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing."
    
    # 3. Count DAGS
    assert dagbag.size() == 3
    print("-----------")
    print("Total DAGS:")
    print(dagbag.size())

    # 4. All loaded DAGS have expected number of tasks
    
    expected_task_count = { # DagID : Expected_Task_Count
        "produce_json": 5,
        "update_db": 3,
        "data_quality": 2,
    } 
    print("-----------")
    print("DAGs and their task counts:")
    for dag_id, dag in dagbag.dags.items(): # dag_id = key, dag = value (airflow object)
        expected_count = expected_task_count[dag_id]
        actual_count = len(dag.tasks)

        assert actual_count == expected_count, f"DAG {dag_id} has unexpected number of tasks. Has {actual_count}, expected {expected_count}"
        print(dag_id, actual_count)