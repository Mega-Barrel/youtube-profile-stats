""" YouTube Profile Watcher Script"""

import os
import json
import logging

import requests
from dotenv import load_dotenv


def extract_user_pages(api_key, user_name):
    """ YT API to get user profile data"""
    response = requests.get(
        "https://www.googleapis.com/youtube/v3/channels",
        params = {
            "key": api_key,
            "forHandle": user_name,
            "part": "contentDetails,contentOwnerDetails,id,localizations,snippet,status,topicDetails"
        }
    )

    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Error while fetching data: {response.status_code}, {response.text}")

if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.environ['yt_api_key']
    CHANNEL_NAME = "@Carberra"
    extract_user_pages(api_key=API_KEY, user_name=CHANNEL_NAME)