""" Main method to hit api """

# System level imports
import os
from dotenv import load_dotenv

# File base imports
from yt_profile_stats.yt_api.api import YouTubeProfileWatcher

if __name__ == "__main__":
    load_dotenv()
    # initialize redis client
    API_KEY = os.environ['YT_API_KEY']
    CHANNEL_NAME = "BeerBiceps"
    yt_pipeline = YouTubeProfileWatcher(
        api_key=API_KEY
    )
    yt_pipeline.run_pipeline(user_name=CHANNEL_NAME)
    print('Finished executing script..')
