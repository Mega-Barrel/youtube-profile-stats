""" Load Module """

from yt_profile_stats.common.db_helper import (
    db_helper,
    insert_data
)

def dump_data_to_db(raw_data):
    """Inserts transformed data into a BigQuery table."""
    # validate table
    db_helper()
    # Wrap data in a list as insert_rows_json expects a list of rows
    rows_to_insert = [raw_data]
    insert_data(
        rows_to_insert=rows_to_insert
    )
    return "Inserted data to BQ"
