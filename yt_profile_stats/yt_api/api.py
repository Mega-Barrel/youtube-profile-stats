""" YouTube Profile Watcher Script"""

# System level imports
import uuid
import json
from time import sleep
from datetime import datetime, timezone

# Package level imports
import requests

# File base imports
from yt_profile_stats.yt_logger.logger import yt_logger
# from yt_profile_stats.db.database import BigQueryOperations

class YouTubeProfileWatcher:
    """ YouTube API Pipeline """

    def __init__(self, api_key, dataset_name, table_name):
        # , dataset_name, table_name
        self.api_key = api_key
        # self.bq_operations = BigQueryOperations(dataset_name=dataset_name, table_name=table_name)

    def run_pipeline(self, user_name):
        """Orchestrates the ETL pipeline for fetching and processing YouTube profile data."""
        retries = 3
        while retries > 0:
            json_data = self.extract_user_pages(user_name)
            resp = json.loads(json_data[0])
            status_code = json_data[1]
            try:
                print('extracting data', retries)
                if status_code == 200:
                    data = self.transform_data(resp=resp, status_code=status_code, username=user_name)
                    # self.dump_data_to_db(data=data)
                    break
                else:
                    print('Error something happned while extracting data')
                    yt_logger.warning("Retrying for user: %s, attempts left: %s", user_name, retries-1)
            except Exception as e:
                data = self.transform_data(resp=resp, status_code=status_code, username=user_name)
                # self.dump_data_to_db(data=data)
                print("Error processing data for user %s: %s", user_name, str(e))
                yt_logger.error("Error processing data for user %s: %s", user_name, str(e))
            retries -= 1
            sleep(5)
        else:
            yt_logger.error("Failed to process data for user %s after multiple attempts.", user_name)

    def extract_user_pages(self, user_name):
        """Fetches YouTube profile data via the YouTube API."""
        yt_logger.info('Attempting API call to YouTube API for username: %s', user_name)
        try:
            response = requests.get(
                "https://www.googleapis.com/youtube/v3/channels",
                params={
                    "key": self.api_key,
                    "forHandle": "@" + user_name,
                    "part": "contentDetails,contentOwnerDetails,id,localizations,snippet,status,topicDetails,statistics"
                },
                timeout=30
            )
            response.raise_for_status()
            yt_logger.info("API Success for %s: %s", user_name, response.text)
            return (response.text, response.status_code, user_name)
        except requests.exceptions.HTTPError as http_err:
            yt_logger.error("HTTP error occurred for %s: %s", user_name, str(http_err))
        except requests.exceptions.RequestException as req_err:
            yt_logger.error("Request error occurred for %s: %s", user_name, str(req_err))
        return (None, 500)

    def transform_data(self, resp, status_code, username):
        """Transforms raw API response data into a structured format."""
        current_time = datetime.now(timezone.utc)

        if status_code == 200 and 'items' not in str(resp):
            yt_data = {
                '_id': str(uuid.uuid4()),  # Generate a unique ID,
                'created_at': current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                'username': username,
                'is_username_found': True if 'items' in str(resp) else False,
                'response': str(resp),
                'response_status_code': status_code,
                'is_success': True,

                'channel_type': None,
                'channel_id': None,
                'channel_description': None,
                'channel_created_at': None,
                'channel_logo': None,
                'channel_country': None,
                'channel_view_count': None,
                'channel_subscriber_count': None,
                'is_hidden_subscriber': None,
                'channel_video_count': None,
                'privacy_channel_type': None,
                'is_channel_linked': None,
                'long_upload_allowed': None,
                'is_kid_safe': None
            }
            yt_logger.debug("No data found for username")
            return yt_data
        else:
            try:
                yt_data = {
                    '_id': str(uuid.uuid4()),  # Generate a unique ID,
                    'created_at': current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                    'username': username if 'title' not in resp else resp['items'][0]['snippet'].get('title'),
                    'is_username_found': True if 'items' in str(resp) else False,
                    'response': str(resp),
                    'response_status_code': status_code,
                    'is_success': True,

                    'channel_type': resp['items'][0].get('kind'),
                    'channel_id': resp['items'][0].get('id'),
                    'channel_description': resp['items'][0]['snippet'].get('description', '').replace('\n\n', ' '),
                    'channel_created_at': resp['items'][0]['snippet'].get('publishedAt'),
                    'channel_logo': resp['items'][0]['snippet']['thumbnails']['high'].get('url'),
                    'channel_country': resp['items'][0]['snippet'].get('country'),
                    'channel_view_count': resp['items'][0]['statistics'].get('viewCount'),
                    'channel_subscriber_count': resp['items'][0]['statistics'].get('subscriberCount'),
                    'is_hidden_subscriber': resp['items'][0]['statistics'].get('hiddenSubscriberCount'),
                    'channel_video_count': resp['items'][0]['statistics'].get('videoCount'),
                    'privacy_channel_type': resp['items'][0]['status'].get('privacyStatus'),
                    'is_channel_linked': resp['items'][0]['status'].get('isLinked'),
                    'long_upload_allowed': resp['items'][0]['status'].get('longUploadsStatus'),
                    'is_kid_safe': resp['items'][0]['status'].get('madeForKids')
                }
                yt_logger.info("API Success")
                return yt_data
            except (KeyError, IndexError) as e:
                yt_logger.error("Error transforming data: %s", str(e))
                raise

    # def dump_data_to_db(self, data):
    #     """Inserts transformed data into a BigQuery table."""
    #     # Wrap data in a list as insert_rows_json expects a list of rows
    #     rows_to_insert = [data]
    #     self.bq_operations.insert_data(rows_to_insert=rows_to_insert)
