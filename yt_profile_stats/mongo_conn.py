"""Module for managing MongoDB Atlas connections and basic operations."""

import os
from typing import Optional

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.database import Database
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv

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
