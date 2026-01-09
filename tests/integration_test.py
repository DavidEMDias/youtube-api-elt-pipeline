import requests
import pytest
import psycopg2

def test_youtube_api_response(airflow_variable):
    """Check if the response that we get from the URL - using real channel_handle and api_key - will be a succesful response"""
    api_key = airflow_variable("api_key")
    channel_handle = airflow_variable("channel_handle")

    url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

    try:
        response = requests.get(url)
        print("\n-----------")
        print(f"Status code: {response.status_code}")
        assert response.status_code == 200

    except requests.RequestException as e:
        pytest.fail(f"Request to YouTube API failed: {e}")


def test_postgres_connection_real_db(real_postgres_connection):
    cursor = None

    try:
        cursor = real_postgres_connection.cursor() # Same as having conn.cursor() because real_postgres_connection returns the connection object
        cursor.execute("SELECT 1;")    # To verify that both the database connection and SQL execution is working
        result = cursor.fetchone() # returns a tuple

        assert result[0] == 1     

    except psycopg2.Error as e:
        pytest.fail(f"Database query failed: {e}")

    finally:
        if cursor:
            cursor.close()


