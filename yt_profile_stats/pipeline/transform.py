""" Transform Module """

import uuid
from datetime import datetime, timezone

def transform_data(resp):
    """Transforms raw API response data into a structured format."""
    current_time = datetime.now(timezone.utc)
    # create a dict to append meta-data
    raw_data = {
        '_id': str(uuid.uuid4()),
        'ingested_time': current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
        'response': str(resp[0]),
        'status_code': resp[1],
        'channel_name': resp[2]
    }

    return raw_data
