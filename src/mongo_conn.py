"""MongoDB Connection and Store Layer for YouTube ETL.

This module manages connections to MongoDB Atlas and provides base storage utilities
for interacting with YouTube channel metadata and analytics collections.
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.server_api import ServerApi
from pymongo.errors import (
    PyMongoError,
    DuplicateKeyError,
    ConnectionFailure
)

# Load environment variables from .env file
load_dotenv()

# Type alias for documents
Document = Dict[str, Any]

# ---------------------- Logging Setup ----------------------

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ---------------------- MongoDB Connection ----------------------

class MongoDBConnection:
    """
    Singleton manager for establishing a MongoDB Atlas connection.
    """

    _client: Optional[MongoClient] = None

    @classmethod
    def get_client(cls) -> MongoClient:
        """
        Lazily initializes and returns a MongoClient instance.

        Raises:
            ConnectionFailure: If unable to connect to MongoDB Atlas.

        Returns:
            MongoClient: Connected MongoDB client.
        """
        if cls._client is None:
            try:
                uri = os.getenv("MONGO_URI", "")
                if not uri:
                    raise ValueError("MONGO_URI not found in environment.")

                cls._client = MongoClient(
                    uri,
                    server_api=ServerApi("1")
                )
                cls._client.admin.command("ping")
                logger.info("MongoDB connection established successfully.")
            except (ConnectionFailure, ValueError) as error:
                logger.error(f"MongoDB connection failed: {error}")
                raise
        return cls._client

    @classmethod
    def get_database(cls, db_name: str) -> Database:
        """
        Returns a MongoDB database instance.

        Args:
            db_name (str): Name of the database.

        Returns:
            Database: MongoDB database object.
        """
        return cls.get_client()[db_name]

# ---------------------- Base Store Classes ----------------------

class BaseMongoStore:
    """
    Base store class that wraps basic read operations on a MongoDB collection.
    """

    def __init__(self, collection: Collection):
        """
        Initializes the store with a MongoDB collection.

        Args:
            collection (Collection): MongoDB collection instance.
        """
        self.collection = collection

    def get_all_documents(self, channel_name = None) -> List[Document]:
        """
        Retrieves all documents in the collection.

        Returns:
            List[Document]: A list of documents, or empty list on failure.
        """
        try:
            if channel_name is None:
                return list(self.collection.find({}))
            else:
                return list(self.collection.find({"channel_name": channel_name}))
        except PyMongoError as error:
            logger.error(f"Error retrieving documents: {error}")
            return []

# ---------------------- YouTube User Metadata Store ----------------------

class YouTubeUserStore(BaseMongoStore):
    """
    Store for managing unique YouTube channel metadata.

    Each document should contain:
    - channel_id: str
    - channel_name: str
    - added_at: datetime
    """

    def __init__(self, collection: Collection):
        """
        Initializes the YouTubeUserStore and ensures a unique index on channel_name.

        Args:
            collection (Collection): The MongoDB collection for users.
        """
        super().__init__(collection)

        try:
            self.collection.create_index("channel_name", unique=True)
        except PyMongoError as error:
            logger.warning(f"Could not create index on 'channel_name': {error}")

    def add_user(self, channel_id: str, channel_name: str) -> bool:
        """
        Adds a new user if they don't already exist.

        Args:
            channel_id (str): Unique YouTube channel ID.
            channel_name (str): Name of the channel.

        Returns:
            bool: True if inserted, False if duplicate or failed.
        """
        try:
            self.collection.insert_one({
                "channel_id": channel_id,
                "channel_name": channel_name.strip(),
                "added_at": datetime.now()
            })
            # logger.info(f"New user added: {channel_name}")
            return f"New user added: {channel_name}"

        except DuplicateKeyError:
            return f"User already exists: {channel_name}"

        except PyMongoError as error:
            return f"Error adding user '{channel_name}': {error}"
