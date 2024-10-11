""" Main method to hit api """

import pandas as pd

# File base imports
from yt_profile_stats.pipeline.extract import extract_user_pages
from yt_profile_stats.pipeline.load import dump_data_to_db

def run_pipeline(channel_name):
    """Orchestrates the ETL pipeline for fetching and processing YouTube profile data."""
    # Extract
    json_data = extract_user_pages(channel_name=channel_name)
    # Load
    dump_data_to_db(resp=json_data)

if __name__ == "__main__":
    df = pd.read_csv('youtubers_list.csv')
    for index, row in df.iterrows():
        channelName = row['YouTube URL']

        run_pipeline(channel_name=channelName.split("/@")[1])
    print('Finished Executing Script..')
