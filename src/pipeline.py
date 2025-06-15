"""YouTube ETL Pipeline: Extracts channel stats from the YouTube API, transforms them,
and loads both user metadata and channel stats into MongoDB.
"""

from datetime import datetime
from typing import Optional, Dict, List, Any

from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from mongo_conn import (
    MongoDBConnection,
    YouTubeUserStore
)

from youtube_connection import YouTubeAPIConnection

class Extract:
    """Handles data extraction from the YouTube API."""

    def __init__(self, client: Optional[Any] = None):
        """Initializes the Extractor with a YouTube API client.

        Args:
            client (Optional[Any]): A pre-initialized YouTube API client.
        """
        self.client = client or YouTubeAPIConnection()

    def fetch_channel_stats(self, channel_id: str) -> List[Dict]:
        """Fetch channel statistics from the YouTube API.

        Args:
            channel_id (str): The YouTube channel ID.

        Returns:
            List[Dict]: Raw API response items (channel data).
        """
        try:
            response = (
                self.client.channels()
                .list(
                    part="id,snippet,statistics",
                    id=channel_id
                )
                .execute()
            )
            return response.get("items", [])
        except Exception as e:
            print(f"Failed to fetch data for {channel_id}: {e}")
            return []

class Transform:
    """Transforms raw YouTube API data into a clean, structured format."""
    def __init__(self, raw_data: List[Dict]):
        """
        Initializes the Transformer.

        Args:
            raw_data (List[Dict]): Raw API response for a single channel.
        """
        self.raw_data = raw_data[0] if raw_data else None

    def transform_channel_stats(self) -> Optional[Dict]:
        """
        Transforms raw YouTube API response into a structured format.

        Returns:
            Optional[Dict]: Structured channel stats dict or None if transformation fails.
        """
        if not self.raw_data:
            return None

        try:
            snippet = self.raw_data.get("snippet", {})
            statistics = self.raw_data.get("statistics", {})
            channel_id = self.raw_data.get("id")

            if not channel_id or not snippet:
                print("Missing required fields: id or snippet.")
                return None

            return {
                "type": self.raw_data.get('kind', 'youtube#channel').replace('#', '-'),
                "channel_id": channel_id,
                "channel_name": snippet.get("title", "Unknown Channel"),
                "channel_description": snippet.get("description", "No description provided"),
                "channel_created_at": snippet.get("publishedAt", None),
                "channel_country": snippet.get("country", "Not Available"),
                "view_count": int(statistics.get("viewCount", 0)),
                "subscriber_count": int(statistics.get("subscriberCount", 0)),
                "video_count": int(statistics.get("videoCount", 0)),
                "fetched_at": self.raw_data.get("fetched_at", datetime.utcnow()),
                "fetched_date": datetime.utcnow().strftime("%Y-%m-%d"),
            }

        except Exception as e:
            print(f"Error in transformation: {e}")
            return None


class Load:
    """Handles loading of transformed data into MongoDB collections."""

    def __init__(self, stats_collection: Collection, users_collection: Collection, user_store: YouTubeUserStore):
        """Initializes the loader.

        Args:
            stats_collection (Collection): MongoDB collection for storing channel stats.
            users_collection (Collection): MongoDB collection for storing user metadata.
            user_store (YouTubeUserStore): Store manager for user operations.
        """
        self.stats_collection = stats_collection
        self.users_collection = users_collection
        self.user_store = user_store

    def insert_channel_stats(self, record: Dict) -> bool:
        """Insert transformed stats and user metadata into MongoDB.

        Args:
            record (Dict): Transformed channel data.

        Returns:
            bool: True if insert was successful, False otherwise.
        """
        if not record:
            return False

        try:
            channel_id = record["channel_id"]
            channel_name = record["channel_name"].strip()

            # Insert user if not already present
            existing_user = self.users_collection.find_one({
                "channel_id": channel_id,
                "channel_name": channel_name
            })

            if not existing_user:
                self.user_store.add_user(
                    channel_id=channel_id,
                    channel_name=channel_name
                )
                print(f"New user added: {channel_name}")
            else:
                print(f"User already exists: {channel_name}")

            # Insert channel stats
            result = self.stats_collection.insert_one(record)
            print(f"Stats inserted with ID: {result.inserted_id}")
            return True

        except PyMongoError as error:
            print(f"MongoDB insert error: {error}")
            return False


def run_etl_for_channel(channel_id: str, stats_collection: Collection, users_collection: Collection, user_store: YouTubeUserStore) -> None:
    """Executes the ETL pipeline for a single YouTube channel.

    Args:
        channel_id (str): The YouTube channel ID.
        stats_collection (Collection): MongoDB stats collection.
        users_collection (Collection): MongoDB users collection.
        user_store (YouTubeUserStore): User store for managing metadata.
    """
    extractor = Extract()
    raw_data = extractor.fetch_channel_stats(channel_id=channel_id)

    transformed = Transform(raw_data=raw_data).transform_channel_stats()
    if transformed:
        Load(
            stats_collection,
            users_collection,
            user_store
        ).insert_channel_stats(transformed)
    else:
        print(f"⚠️ Skipped channel {channel_id}: no data or failed transformation.")


def main() -> None:
    """Main function to execute the ETL pipeline for all registered YouTube channels."""
    # Initialize MongoDB
    db = MongoDBConnection.get_database("st-db")
    stats_collection = db["st-youtube_analytics"]
    users_collection = db["youtube_users"]
    user_store = YouTubeUserStore(users_collection)

    users = user_store.get_all_documents()
    print(f"Found {len(users)} registered users to process.\n")

    for user in users:
        channel_id = user.get('channel_id')
        run_etl_for_channel(
            channel_id=channel_id,
            stats_collection=stats_collection,
            users_collection=users_collection,
            user_store=user_store
        )

if __name__ == "__main__":
    main()
