import requests
import json
from datetime import date

# import os
# from dotenv import load_dotenv
# load_dotenv(dotenv_path="./.env")

from airflow.decorators import task # for TaskFlow API
from airflow.models import Variable



# Without airflow
#API_KEY = os.environ["API_KEY"]  # raises KeyError if missing
#CHANNEL_HANDLE = "MrBeast"

# With airflow
API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
MAX_RESULTS = 50

@task
def get_playlist_id() -> str:
    """Get the uploads playlist ID for a YouTube channel using its handle."""

    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        response.raise_for_status()  # Raise an error for bad status codes

        data = response.json()
        # print(json.dumps(data, indent=4)) # Pretty print JSON response

        channel_items = data["items"][0]
        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        #print(f"Channel Uploads Playlist ID: {channel_playlistId}")

        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise e
    
@task
def get_video_ids(playlist_id : str) -> list:
    """Get all video IDs from a YouTube playlist."""

    video_ids = [] # List to store video IDs
    pageToken = None  # Initialize pageToken to None for the first request

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULTS}&playlistId={playlist_id}&key={API_KEY}"
        
    # Get nextPageToken and copy into pageToken parameter to get next page of results - not needed for first page/iteration
    try: 
        while True:
            url = base_url

            if pageToken: # For first iteration/page, pageToken is None so condition is False
                url += f"&pageToken={pageToken}" # Append pageToken parameter on main url for subsequent requests/pages

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []): # [] as fail-safe. After 50 items jumps out of loop.
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)

            pageToken = data.get("nextPageToken") # Get nextPageToken for the next iteration/page

            if not pageToken: # If there is no nextPageToken, we have reached the last page
                break

        return video_ids # Return the list of video IDs after processing all pages

    except requests.exceptions.RequestException as e:
        raise e    


@task
def extract_video_data(video_ids: list) -> list:

    extracted_data = []

    def batch_list(video_id_list, batch_size):
        """Due to the limit of 50 video IDs per request, this function yields batches of video IDs."""
        for video_id in range(0, len(video_id_list), batch_size): # Step through the list in increments of batch_size (e.g., first 0, then 50, then 100, etc.)
            yield video_id_list[video_id: video_id + batch_size] # Yield a slice of the list from the current index to current index + batch_size (e.g. 0 to 50, then 50 to 100, etc.)


    try:
        for batch in batch_list(video_id_list = video_ids, batch_size = MAX_RESULTS): # batch_list returns batches of video IDs
            video_ids_str = ",".join(batch) # Join the list of video IDs into a comma-separated string
             
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=statistics&part=snippet&part=contentDetails&id={video_ids_str}&key={API_KEY}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):

                # Break down the JSON response into its components
                video_id = item["id"]
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]

                # Create a dictionary to store the extracted data for each video
                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount", None), # It can happen that views, likes and comments are not visible
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None)
                }

                extracted_data.append(video_data)

        return extracted_data
    
    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):
    file_path = f"./data/YT_video_data_{date.today()}.json" # File path with today's date (e.g., YT_video_data_2023-10-05.json) - ELT run date 

    with open(file_path, mode="w", encoding="utf-8") as json_outfile: # UTF-8 encoding to support special characters
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False) # Pretty print with indent of 4 spaces, ensure_ascii=False to support special characters


if __name__ == "__main__": 
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_video_data = extract_video_data(video_ids)
    save_to_json(extracted_video_data)

