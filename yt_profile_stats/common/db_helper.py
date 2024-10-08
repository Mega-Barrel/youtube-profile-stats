""" BigQuery Helper Module """

import os
from time import sleep
from dotenv import load_dotenv

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

load_dotenv()
# Get the path to the service account file from the environment variable
__CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
__PROJECT_ID = os.getenv("PROJECT_ID")
__DATASET_NAME = os.getenv("DATASET_ID")
__TABLE_NAME = os.getenv("TABLE_ID")
__TABLE_ID = f"{__PROJECT_ID}.{__DATASET_NAME}.{__TABLE_NAME}"
credentials = service_account.Credentials.from_service_account_file(
    __CREDENTIALS
)

_client = bigquery.Client(
    credentials=credentials,
    project=__PROJECT_ID
)

def db_helper() -> None:
    """Initializes the BigQuery client and checks for dataset/table existence."""

    if not dataset_exists():
        create_dataset()

    if not table_exists():
        create_partitioned_table()
        sleep(4)

def dataset_exists() -> bool:
    """Checks if a dataset exists in the project."""
    dataset_id = f"{__PROJECT_ID}.{__DATASET_NAME}"
    try:
        _client.get_dataset(dataset_id)
        return True
    except NotFound:
        return False

def create_dataset() -> None:
    """Creates a new dataset if it does not exist."""
    dataset_id = f"{__PROJECT_ID}.{__DATASET_NAME}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "asia-south1"
    _client.create_dataset(dataset, timeout=30)

def table_exists() -> bool:
    """Checks if a table exists in the dataset."""
    try:
        _client.get_table(__TABLE_ID)
        return True
    except NotFound:
        return False

def create_partitioned_table() -> None:
    """Creates a new table with a custom schema and time-based partitioning."""
    schema = [
        bigquery.SchemaField(
            "_id",
            "STRING",
            mode="REQUIRED",
            description="Unique ID call made to Youtube API"
        ),
        bigquery.SchemaField(
            "ingested_time",
            "TIMESTAMP",
            mode="REQUIRED",
            description="Timestamp of the API call"
        ),
        bigquery.SchemaField(
            "response",
            "STRING",
            mode="REQUIRED",
            description="API Response Data"
        ),
        bigquery.SchemaField(
            "status_code",
            "STRING",
            mode="REQUIRED",
            description="API Status Code"
        ),
        bigquery.SchemaField(
            "channel_name",
            "STRING",
            description="YT Channel Name"
        )
    ]

    table = bigquery.Table(__TABLE_ID, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingested_time",
    )
    table = _client.create_table(table)
    return

def insert_data(rows_to_insert: list[dict]) -> None:
    """Inserts data into the table."""
    errors = _client.insert_rows_json(__TABLE_ID, rows_to_insert)
    if not errors:
        return True
    else:
        return False
