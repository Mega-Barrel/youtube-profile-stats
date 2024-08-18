""" Main method to hit api """

# System level imports
import os
from dotenv import load_dotenv

# File base imports
from yt_profile_stats.yt_api.api import pipeline

if __name__ == "__main__":
    load_dotenv()
    # initialize redis client
    API_KEY = os.environ['yt_api_key']
    CHANNEL_NAME = "@MrBeast"
    yt_queue = []
    pipeline(api_key=API_KEY, user_name=CHANNEL_NAME, yt_queue=yt_queue)

    print('Finished executing script..')
    print(yt_queue)
