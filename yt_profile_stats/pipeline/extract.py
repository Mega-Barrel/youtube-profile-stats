""" Extract Method """

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

def extract_user_pages(channel_name):
    """Fetches YouTube profile data via the YouTube API."""
    try:
        response = requests.get(
            "https://www.googleapis.com/youtube/v3/channels",
            params={
                "key": os.environ['YT_API_KEY'],
                "forHandle": "@" + channel_name,
                "part": "contentDetails,contentOwnerDetails,id,localizations,snippet,status,topicDetails,statistics"
            },
            timeout=30
        )
        response.raise_for_status()
        return (
            json.loads(response.text),
            response.status_code,
            channel_name
        )
    except requests.exceptions.ConnectionError as err:
        return SystemExit(err)
