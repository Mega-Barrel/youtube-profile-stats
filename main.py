""" Main method to hit api """

# System level imports

# File base imports
from yt_profile_stats.pipeline.extract import extract_user_pages
from yt_profile_stats.pipeline.transform import transform_data
from yt_profile_stats.pipeline.load import dump_data_to_db

def run_pipeline(channel_name):
    """Orchestrates the ETL pipeline for fetching and processing YouTube profile data."""
    # Extract
    json_data = extract_user_pages(channel_name=channel_name)
    # Load
    dump_data_to_db(raw_data=transform_data(resp=json_data))

if __name__ == "__main__":
    CHANNEL_NAME = 'BeerBiceps'
    run_pipeline(channel_name=CHANNEL_NAME)
    print('Finished Executing Script..')
