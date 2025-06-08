"""Module for managing MongoDB Atlas connections and basic operations."""

import os

from typing import (
    List,
    Dict,
    Optional
)

from dotenv import load_dotenv
from pymongo.database import Database
from pymongo.server_api import ServerApi
from pymongo.mongo_client import (
    MongoClient,
    PyMongoError
)
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure

# Load environment variables from .env file
load_dotenv()

class MongoDBConnection:
    """Class to manage a singleton MongoDB connection."""

    _instance: Optional[MongoClient] = None
    URI: str = os.getenv("MONGO_URI", "")

    @staticmethod
    def get_client() -> MongoClient:
        """Get or create a MongoDB client connection.

        Returns:
            MongoClient: The MongoDB client instance.

        Raises:
            ConnectionFailure: If connection to MongoDB fails.
        """
        if MongoDBConnection._instance is None:
            try:
                MongoDBConnection._instance = MongoClient(
                    MongoDBConnection.URI,
                    server_api=ServerApi("1"),
                )
                MongoDBConnection._instance.admin.command("ping")
                print("Pinged your deployment. You successfully connected to MongoDB!")
            except ConnectionFailure as error:
                print(f"Failed to connect to MongoDB: {error}")
                raise
        return MongoDBConnection._instance

    @staticmethod
    def get_database(db_name: str) -> Database:
        """Retrieve a MongoDB database instance.

        Args:
            db_name (str): The name of the database to access.

        Returns:
            Database: The MongoDB database instance.
        """
        client = MongoDBConnection.get_client()
        return client[db_name]

class QueryMongoDB:

    """Class to Query MongoDB collection."""
    def __init__(self, collection: Collection):
        self.collection = collection

    def get_all_documents(self) -> List[Dict]:
        """Load all documents from the collection into memory."""
        try:
            return list(self.collection.find({}))
        except PyMongoError as error:
            print(f"Error loading documents: {error}")
            return []

    def get_distinct_channel_names(self, all_docs: List[Dict]) -> List[str]:
        """Return sorted list of distinct channel_name values from Collections."""
        try:
            names = {
                doc.get("channel_name", "").strip() for doc in all_docs if "channel_name" in doc
            }
            return sorted(names)
        except Exception as error:
            print(f"Error extracting channel names: {error}")
            return []

    def get_documents_by_channel_name(self, all_docs: List[Dict], channel_name: str) -> Optional[List[Dict]]:
        """
        Query and return the document matching a channel_name.

        Args:
            all_docs (List[Dict]): Raw MongoDB Document
            channel_name (str): The name of the YouTube channel to search for.

        Returns:
            dict or None: A single matching document, or None if not found.
        """
        try:
            result = [doc for doc in all_docs if doc.get("channel_name") == channel_name]
            if result:
                return result
            print("No documents found for the given channel name.")
            return None
        except Exception as error:
            print(f"Error filtering data: {error}")
            return None
