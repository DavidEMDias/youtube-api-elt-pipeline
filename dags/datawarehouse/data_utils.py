# Functions related to database connections and operations (creating tables and schemas)

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

TABLE = "yt_api"

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db") #postgres_db_yt_elt comes from AIRFLOW_CONN_POSTGRES_DB_YT_ELT in Airflow Connections docker-compose.yaml
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor) # Changes how results are returned (as dictionaries instead of tuples) when we execute queries
    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()


def create_schema(schema):

    conn, cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    
    cur.execute(schema_sql)

    conn.commit() # Commit changes to the database

    close_conn_cursor(conn, cur)


def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{TABLE} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
                );
            """
    else: # For the core schema
        table_sql = f"""
                  CREATE TABLE IF NOT EXISTS {schema}.{TABLE} (
                      "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                      "Video_Title" TEXT NOT NULL,
                      "Upload_Date" TIMESTAMP NOT NULL,
                      "Duration" TIME NOT NULL,
                      "Video_Type" VARCHAR(10) NOT NULL,
                      "Video_Views" INT,
                      "Likes_Count" INT,
                      "Comments_Count" INT    
                  ); 
              """
    cur.execute(table_sql)

    conn.commit()

    close_conn_cursor(conn, cur)


# Helpful to loop throught the rows of data inside tables
def get_video_ids(cur, schema):

    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{TABLE};""")
    ids = cur.fetchall() # Returns a list of dictionaries because of RealDictCursor where the key is always "Video_ID" ex: [{"Video_ID": "abc123"}, {"Video_ID": "def456"}]

    video_ids = [row["Video_ID"] for row in ids] # Extract Video_ID values from the list of dictionaries. Output: ["abc123", "def456"]

    return video_ids