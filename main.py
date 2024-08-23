""" Main method to hit api """

# System level imports
import os
from dotenv import load_dotenv

# File base imports
from yt_profile_stats.yt_api.api import YouTubeProfileWatcher

if __name__ == "__main__":
    load_dotenv()
    # initialize redis client
    API_KEY = os.environ['yt_api_key']
    CHANNEL_NAME = "@BeerBiceps"
    DATASET_NAME = 'yt_stats'
    TABLE_NAME = 'yt_profile_stats_test'
    yt_pipeline = YouTubeProfileWatcher(
        api_key=API_KEY,
        dataset_name=DATASET_NAME,
        table_name=TABLE_NAME
    )
    yt_pipeline.run_pipeline(user_name=CHANNEL_NAME)
    print('Finished executing script..')
