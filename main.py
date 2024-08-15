""" YouTube Profile Watcher Script"""

import os
import json
import logging

import redis
import requests
from dotenv import load_dotenv

# redis_client = redis.Redis(
#     host='localhost',
#     port=6379,
#     db=0
# )

# def extract_user_pages(api_key, user_name):
#     """ YT API to get user profile data"""
#     data = redis_client.get(user_name)
#     if not data:
#         print('cache not found, attempting call to YT API.')
#         response = requests.get(
#             "https://www.googleapis.com/youtube/v3/channels",
#             params = {
#                 "key": api_key,
#                 "forHandle": user_name,
#                 "part": "contentDetails,contentOwnerDetails,id,localizations,snippet,status,topicDetails,statistics"
#             }
#         )
#         if response.status_code == 200:
#             redis_client.set(user_name, response.text)
#             return response.text
#         print(f"Error while fetching data: {response.status_code}, {response.text}")
#     # return data from redis cache
#     print('fetching data from redis cache.')
#     return data

def extract_user_pages(api_key, user_name):
    """ YT API to get user profile data"""
    response = requests.get(
        "https://www.googleapis.com/youtube/v3/channels",
        params = {
            "key": api_key,
            "forHandle": user_name,
            "part": "contentDetails,contentOwnerDetails,id,localizations,snippet,status,topicDetails,statistics"
        }
    )
    if response.status_code == 200:
        return response.text
    print(f"Error while fetching data: {response.status_code}, {response.text}")

def transform_data(response):
    """ Method to clean and iterate user data """
    resp = json.loads(response)
    yt_data = {
        'channel_type': resp.get('items')[0]['kind'],
        'channel_name': resp.get('items')[0]['snippet']['title'],
        'channel_id': resp.get('items')[0]['id'],
        'channel_description': resp.get('items')[0]['snippet']['description'].replace('\n\n', ' '),
        'channel_created_at': resp.get('items')[0]['snippet']['publishedAt'],
        'channel_logo': resp.get('items')[0]['snippet']['thumbnails']['high']['url'],
        'channel_country': resp.get('items')[0]['snippet']['country'],
        'channel_view_count': resp.get('items')[0]['statistics']['viewCount'],
        'channel_subscriber_count': resp.get('items')[0]['statistics']['subscriberCount'],
        'is_hidden_subscriber': resp.get('items')[0]['statistics']['hiddenSubscriberCount'],
        'channel_video_count': resp.get('items')[0]['statistics']['videoCount'],
        'privacy_channel_type': resp.get('items')[0]['status']['privacyStatus'],
        'is_channel_linked': resp.get('items')[0]['status']['isLinked'],
        'long_upload_allowed': resp.get('items')[0]['status']['longUploadsStatus'],
        'is_kid_safe': resp.get('items')[0]['status']['madeForKids']
    }
    print(yt_data)

if __name__ == "__main__":
    load_dotenv()
    # initialize redis client
    API_KEY = os.environ['yt_api_key']
    CHANNEL_NAME = "@MrBeast"

    raw_data = extract_user_pages(api_key=API_KEY, user_name=CHANNEL_NAME)
    transform_data(raw_data)
