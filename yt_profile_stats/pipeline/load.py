""" Load Module """

import uuid
from datetime import datetime, timezone

from yt_profile_stats.common.db_helper import (
    db_helper,
    insert_data
)

def dump_data_to_db(resp):
    """Inserts RAW API data into a BigQuery table."""
    current_time = datetime.now(timezone.utc)

    # create a dict to append meta-data
    raw_data = {
        '_id': str(uuid.uuid4()),
        'ingested_date': current_time.strftime('%Y-%m-%d'),
        'response': str(resp[0]),
        'status_code': resp[1],
        'channel_name': resp[2]
    }

    # validate table
    db_helper()

    # Wrap data in a list as insert_rows_json expects a list of rows
    rows_to_insert = [raw_data]
    return insert_data(
        rows_to_insert=rows_to_insert
    )
