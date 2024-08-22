""" YouTube Profile Watcher Script"""

# System level imports
import json

# Package level imports
import requests

# File base imports
from yt_profile_stats.yt_logger.logger import yt_logger

def pipeline(api_key, user_name):
    """ Function to call extract_user_pages and transform_data function """
    json_data = extract_user_pages(api_key=api_key, user_name=user_name)
    data = transform_data(response=json_data)
    dump_data_to_db(data=data)

def extract_user_pages(api_key, user_name):
    """ YT API to get user profile data"""
    yt_logger.info('Attempting API call to YOUTUBE_API for username: %s', user_name)
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
        yt_logger.info("%s - API Success, received data: %s", response.status_code, response.text)
        return (response.text, response.status_code)
    else:
        yt_logger.error("Error while fetching data: %s, %s", response.status_code, response.text)
        return (response.text, response.status_code)

def transform_data(response) -> None:
    """ Method to clean and iterate user data """
    resp = json.loads(response[0])
    status_code = response[1]

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

    final_data = {
        '_id': '1',
        'created_at': '2024-03-12 00:22:11',
        'raw_response': str(response[0]),
        'clean_response': yt_data,
        'response_status_code': status_code,
        'is_success': True if status_code == 200 else False
    }

    # return json object
    return final_data

def dump_data_to_db(data: list):
    """ Method to dump raw data to BQ """
    try:
        with open('data/data.json', 'r', encoding='utf-8') as file:
            existing_data = json.load(file)
    except FileNotFoundError:
        existing_data = {}

    # Merge the new data with the existing data (assuming both are lists)
    existing_data.update(data)

    # Write the updated data back to the JSON file (creates the file if it doesn't exist)
    with open('data/data.json', 'w', encoding='utf-8') as file:
        json.dump(existing_data, file, indent=4)
    yt_logger.info("Data updated successfully!")
