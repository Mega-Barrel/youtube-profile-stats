""" YouTube Profile Watcher Script"""

# System level imports
import json

# Package level imports
import requests

# File base imports
from yt_profile_stats.yt_logger.logger import yt_logger

def pipeline(api_key, user_name, yt_queue):
    """ Function to call extract_user_pages and transform_data function """
    json_data = extract_user_pages(api_key=api_key, user_name=user_name)
    transform_data(response=json_data, queue=yt_queue)

def extract_user_pages(api_key, user_name):
    """ YT API to get user profile data"""
    yt_logger.info(f'Attempting call to yt_api for username: {user_name}.')
    response = requests.get(
        "https://www.googleapis.com/youtube/v3/channels",
        params = {
            "key": api_key,
            "forHandle": user_name,
            "part": "contentDetails,contentOwnerDetails,id,localizations,snippet,status,topicDetails,statistics"
        },
        timeout=500
    )
    if response.status_code == 200:
        yt_logger.info(f'API Success, received data: {response.text}')
        return response.text
    yt_logger.error(f"Error while fetching data: {response.status_code}, {response.text}")

def transform_data(response, queue) -> None:
    """ Method to clean and iterate user data """
    resp = json.loads(response)
    # yt_data = {
    #     'channel_type': resp.get('items')[0]['kind'],
    #     'channel_name': resp.get('items')[0]['snippet']['title'],
    #     'channel_id': resp.get('items')[0]['id'],
    #     'channel_description': resp.get('items')[0]['snippet']['description'].replace('\n\n', ' '),
    #     'channel_created_at': resp.get('items')[0]['snippet']['publishedAt'],
    #     'channel_logo': resp.get('items')[0]['snippet']['thumbnails']['high']['url'],
    #     'channel_country': resp.get('items')[0]['snippet']['country'],
    #     'channel_view_count': resp.get('items')[0]['statistics']['viewCount'],
    #     'channel_subscriber_count': resp.get('items')[0]['statistics']['subscriberCount'],
    #     'is_hidden_subscriber': resp.get('items')[0]['statistics']['hiddenSubscriberCount'],
    #     'channel_video_count': resp.get('items')[0]['statistics']['videoCount'],
    #     'privacy_channel_type': resp.get('items')[0]['status']['privacyStatus'],
    #     'is_channel_linked': resp.get('items')[0]['status']['isLinked'],
    #     'long_upload_allowed': resp.get('items')[0]['status']['longUploadsStatus'],
    #     'is_kid_safe': resp.get('items')[0]['status']['madeForKids']
    # }

    # return json object
    queue.append(resp)

def dump_data_to_db(data: list):
    """ Method to dump raw data to BQ """
    pass
