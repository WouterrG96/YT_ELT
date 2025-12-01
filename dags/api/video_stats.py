import requests
import json
# import os
# from dotenv import load_dotenv
from datetime import date

from airflow.decorators import task
from airflow.models import Variable

# # Load environment variables from a local .env file (so you don't hardcode secrets)
# load_dotenv(dotenv_path="./.env")

# Read the YouTube Data API key from environment variables
API_KEY = Variable.get("API_KEY")

# Read the YouTube channel handle you want to query (e.g., https://youtube.com/@MrBeast)
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")

# Max items per page/batch for YouTube Data API calls (most endpoints cap at 50)
maxResults = 50

@task
def get_playlist_id():
    """
    Fetch the channel's 'uploads' playlist ID.
    On YouTube, every channel effectively has an 'uploads' playlist containing all uploaded videos.
    """
    try:
        # Query the Channels endpoint, requesting contentDetails for the channel identified by handle
        url = (
            f"https://youtube.googleapis.com/youtube/v3/channels"
            f"?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        )

        # Perform the HTTP GET request
        response = requests.get(url)

        # Raise an exception for 4xx/5xx responses
        response.raise_for_status()

        # Parse JSON response body into a Python dict
        data = response.json()
        # print(json.dumps(data, indent=4))  # Useful for debugging the raw API response

        # The first (and typically only) item corresponds to the channel
        channel_items = data["items"][0]

        # Extract the playlist ID for uploads from relatedPlaylists
        channel_playlistID = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        return channel_playlistID

    # Catch network errors, invalid responses, timeouts, etc.
    except requests.exceptions.RequestException as e:
        raise e

@task
def get_video_ids(playlistId):
    """
    Given an uploads playlist ID, fetch all video IDs from that playlist,
    walking across pages using nextPageToken until exhausted.
    """
    # List to accumulate all video IDs
    video_ids = []

    # Pagination token (None means "first page")
    pageToken = None

    # Base PlaylistItems endpoint URL for contentDetails (contains videoId)
    base_url = (
        f"https://youtube.googleapis.com/youtube/v3/playlistItems"
        f"?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}"
    )

    try:
        while True:
            # Start with base URL each loop iteration
            url = base_url

            # If we have a nextPageToken from the previous response, request that page
            if pageToken:
                url += f"&pageToken={pageToken}"

            # Perform the HTTP GET request
            response = requests.get(url)
            response.raise_for_status()

            # Parse JSON response
            data = response.json()

            # Extract videoId from each playlist item
            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)

            # Update page token; if absent, we're done
            pageToken = data.get("nextPageToken")
            if not pageToken:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e

@task
def extract_video_data(video_ids):
    """
    Given a list of video IDs, call the Videos endpoint in batches to fetch:
    - snippet (title, publishedAt)
    - contentDetails (duration)
    - statistics (views, likes, comments)

    Returns: list of dicts, one per video.
    """
    extracted_data = []

    def batch_list(video_id_lst, batch_size):
        """
        Yield successive slices (batches) of video IDs to avoid exceeding API limits.
        """
        for video_id in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[video_id : video_id + batch_size]

    try:
        # The videos endpoint allows query by multiple IDs: id=a,b,c...
        for batch in batch_list(video_ids, maxResults):
            # Join IDs into a comma-separated string for the request
            video_ids_str = ",".join(batch)

            # Request multiple "parts" so we get all needed fields in one call
            url = (
                f"https://youtube.googleapis.com/youtube/v3/videos"
                f"?part=contentDetails&part=snippet&part=statistics"
                f"&id={video_ids_str}&key={API_KEY}"
            )

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            # Parse each returned video object into a friendlier dict
            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]

                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],  # ISO 8601 duration (e.g., PT12M34S)
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None),
                }

                # NOTE: This should be inside the loop to append each video's data.
                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e
    
@task
def save_to_json(extracted_data):
    """Save extracted data to a date-stamped JSON file in ./data/.

    The filename includes today's date (e.g., YT_data_2025-12-01.json).
    The JSON is pretty-printed (indentation) and written as UTF-8 while
    preserving non-ASCII characters.
    """
    # Build an output path with today's date so each run creates a new file.
    file_path = f"./data/YT_data_{date.today()}.json"

    # Open (or create) the file in write mode using UTF-8 encoding.
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        # Serialize Python data structures to JSON:
        # - indent=4 makes it human-readable
        # - ensure_ascii=False keeps unicode characters intact (e.g., emoji, accents)
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    # 1) Get the uploads playlist for the configured channel handle
    playlistId = get_playlist_id()

    # 2) Fetch every video ID from that uploads playlist
    video_ids = get_video_ids(playlistId)

    # 3) Fetch details/statistics for those videos
    video_data = extract_video_data(video_ids)

    # 4) Save to JSON in data folder
    save_to_json(video_data)
