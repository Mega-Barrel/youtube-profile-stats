""" Youtube ETL Pipeline """

from datetime import datetime
from typing import Optional, Dict

from pymongo.errors import PyMongoError

from src.mongo_conn import MongoDBConnection
from youtube_connection import YouTubeAPIConnection

class Extract:
    """Class to extract data using the YouTube API client."""

    def __init__(self, client=None):
        self.client = client or YouTubeAPIConnection()

    def fetch_channel_stats(self, channel_name: str) -> Dict:
        """ Method to Fetch Channel Stats"""
        response = (
            self.client.channels()
            .list(
                part="id,localizations,snippet,topicDetails,statistics",
                forHandle="@" + channel_name
            )
            .execute()
        )

        items = response.get("items", [])
        if not items:
            return {}
        return items

class Transform:
    """Class to Transform raw data."""

    def __init__(self, raw_data):
        self.raw_data = raw_data[0]

    def transform_channel_stats(self) -> Optional[Dict]:
        """Transforms raw YouTube API data into a clean format for storage."""
        if not self.raw_data:
            return None
        return {
            "type": self.raw_data['kind'].replace('#', '-'),
            "channel_id": self.raw_data["id"],
            "channel_name": self.raw_data["snippet"]["title"],
            "channel_description": self.raw_data["snippet"]["description"],
            "channel_created_at": self.raw_data["snippet"]["publishedAt"],
            "channel_country": self.raw_data["snippet"]["country"],
            "view_count": int(self.raw_data["statistics"]["viewCount"]),
            "subscriber_count": int(self.raw_data["statistics"]["subscriberCount"]),
            "video_count": int(self.raw_data["statistics"]["videoCount"]),
            "fetched_at": self.raw_data.get("fetched_at", datetime.now()),
            "fetched_date": datetime.now().strftime("%Y-%m-%d"),
        }

class Load:
    """Class to load transformed data into MongoDB."""

    def __init__(self):
        # Get database and collection
        self.__database = MongoDBConnection.get_database("st-db")
        self.__collection = self.__database["st-youtube_analytics"]

    def insert_channel_stats(self, record: dict) -> bool:
        """Insert transformed YouTube channel stats into MongoDB."""
        if not record:
            return False
        try:
            result = self.__collection.insert_one(record)
            print(f"Inserted document with ID:{result.inserted_id}, {result.acknowledged}")
            return True
        except PyMongoError as error:
            print(f"Error inserting data: {error}")
            return False

def main(channel_name):
    """ Main Class to execute ETL pipeline"""
    extract = Extract()
    data = extract.fetch_channel_stats(channel_name=channel_name)
    extracted_data = Transform(raw_data=data).transform_channel_stats()
    Load().insert_channel_stats(record=extracted_data)

if __name__ == "__main__":
    CHANNEL_NAME = 'AnuragSalgaonkar'
    main(channel_name = CHANNEL_NAME)
