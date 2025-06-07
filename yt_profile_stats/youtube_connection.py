"""Module for managing MongoDB, YouTubeAPI classes"""

import os
from typing import Optional
from dotenv import load_dotenv

# Google Package
from googleapiclient.discovery import build

# Load environment variables from .env file
load_dotenv()

class YouTubeAPIConnection:
    """Singleton class for managing YouTube API connection."""
    _instance: Optional[object] = None

    def __new__(cls):
        if cls._instance is None:
            api_key = os.getenv("YOUTUBE_API_KEY")
            cls._instance = build("youtube", "v3", developerKey=api_key)
        return cls._instance
