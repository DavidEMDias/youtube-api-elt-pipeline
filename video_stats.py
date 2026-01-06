import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

API_KEY = os.environ["API_KEY"]  # raises KeyError if missing
CHANNEL_HANDLE = "MrBeast"

def get_playlist_id():

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
    
if __name__ == "__main__": 
    get_playlist_id()

