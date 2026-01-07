import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_data():

    file_path = f"./data/YT_video_data_{date.today().json}"

    try:
        logger.info(f"Processing file: YT_video_data_{date.today()}") 

        with open(file_path, "r", encoding="utf-8") as raw_data:
           data = json.load(raw_data) # Effectively loading the entire contents of the JSON file into memory - small = no issues / big = problems if memory is limited
                                      
        return data 

    except FileNotFoundError: # Possible failures: File not being found or JSON data is invalid
       logger.error(f"File not found: {file_path}") # logger.error logs message with level error
       raise
    except json.JSONDecodeError:
       logger.error(f"Invalid JSON in file: {file_path}")
       raise