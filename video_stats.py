import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")


API_KEY = os.environ["API_KEY"]  # raises KeyError if missing
CHANNEL_HANDLE = "MrBeast"
MAX_RESULTS = 50

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

            for item in data.get("items", []): # [] as fail-safe.
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)

                pageToken = data.get("nextPageToken") # Get nextPageToken for the next iteration/page

            if not pageToken: # If there is no nextPageToken, we have reached the last page
                break

        return video_ids # Return the list of video IDs after processing all pages

    except requests.exceptions.RequestException as e:
        raise e    
    
if __name__ == "__main__": 
    playlist_id = get_playlist_id()
    get_video_ids(playlist_id)

