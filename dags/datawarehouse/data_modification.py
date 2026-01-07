import logging

logger = logging.getLogger(__name__)
TABLE = "yt_api"

def insert_rows(cur, conn, schema, row): # row is a dictionary containing the data to be inserted

    try:
        if schema == "staging":

            video_id = "video_id" # For logging purposes

            cur.executa(
                f""" 
                INSERT INTO {schema}.{TABLE} ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
                """, row
            )

        else:

            video_id = "Video_ID" # How we refered in the staging layer

            cur.execute(
                f"""
                INSERT INTO {schema}.{TABLE}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s)
                """, row
            ) # We will read data from the previous staging layer

        conn.commit()

        logger.info(f"Inserted row with Video_ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error inserting row with Video_ID: {row[video_id]}")
        raise e