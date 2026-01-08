from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():

    schema = "staging"

    conn = None
    cur = None

    try:

        conn, cur = get_conn_cursor() # Set up connection and cursor variables

        YT_data = load_data() # Load data from JSON file

        create_schema(schema=schema)
        create_table(schema=schema)

        video_ids_in_table = get_video_ids(cur,schema) # Get existing video IDs from the staging table

        for row in YT_data:

            if len(video_ids_in_table) == 0:
                insert_rows(cur,conn,schema,row)
            else:
                # UPSERT logic
                if row["video_id"] in video_ids_in_table:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur,conn,schema,row)

        ids_in_json = {row["video_id"] for row in YT_data } # ids_in_json Set  

        # ids_to_delete = {video_id for video_id in video_ids if video_id not in ids_in_json}
        ids_to_delete = set(video_ids_in_table) - ids_in_json # Set difference to find IDs to delete
        
        if ids_to_delete:
            delete_rows(cur,conn,schema,ids_to_delete)

        logger.info(f"{schema} table update completed")
    
    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table - {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn,cur)




@task
def core_table():
    
    schema = "core"

    conn = None
    cur = None

    try:
        conn, cur = get_conn_cursor()
        
        create_schema(schema)
        create_table(schema)

        video_ids_in_table = get_video_ids(cur,schema)

        current_video_ids = set()

        cur.execute(f"SELECT * FROM staging.{table};") # For small table sizes this works, for bigger sizes process the data in batches with a specific batch size depending on data size.
        rows = cur.fetchall()

        for row in rows:

            current_video_ids.add(row["Video_ID"]) # This are video IDs from staging table

            if len(video_ids_in_table) == 0:
                transformed_row = transform_data(row) # Transforms duration column and adds Video_Type column
                insert_rows(cur, conn, schema, transformed_row)
            else:
                transformed_row = transform_data(row)

                if transformed_row["Video_ID"] in video_ids_in_table:
                    update_rows(cur, conn, schema, transformed_row)
                else:
                    insert_rows(cur, conn, schema, transformed_row)

        ids_to_delete = set(video_ids_in_table) - current_video_ids # Set difference to find IDs to delete between core table and staging table.

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
        
        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table - {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn,cur)