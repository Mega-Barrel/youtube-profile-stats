""" Big Query DB for handling table CURD operations"""

import os
from time import sleep

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

# from yt_profile_stats.yt_logger.logger import yt_logger

class BigQueryOperations:
    """Encapsulates operations for interacting with BigQuery tables."""

    def __init__(self, dataset_name, table_name) -> None:
        """Initializes the BigQuery client and checks for dataset/table existence."""
        load_dotenv()
        self._project_id = os.getenv("project_id")
        self._dataset_name = dataset_name
        self._table_name = table_name
        self._table_id = f"{self._project_id}.{self._dataset_name}.{self._table_name}"
        self.credentials = service_account.Credentials.from_service_account_file(
            "configs/service_account.json"
        )
        self._client = bigquery.Client(
            credentials=self.credentials,
            project=self._project_id
        )

        if not self.dataset_exists():
            self.create_dataset()

        if not self.table_exists():
            self.create_partitioned_table()

    def dataset_exists(self) -> bool:
        """Checks if a dataset exists in the project."""
        dataset_id = f"{self._project_id}.{self._dataset_name}"
        try:
            self._client.get_dataset(dataset_id)
            print(f"Dataset {self._dataset_name} already exists.")
            return True
        except NotFound:
            print(f"Dataset {self._dataset_name} does not exist.")
            return False

    def create_dataset(self) -> None:
        """Creates a new dataset if it does not exist."""
        dataset_id = f"{self._project_id}.{self._dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "asia-south1"
        self._client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset_id}.")

    def table_exists(self) -> bool:
        """Checks if a table exists in the dataset."""
        try:
            self._client.get_table(self._table_id)
            print(f"Table {self._table_id} already exists.")
            return True
        except NotFound:
            print(f"Table `{self._table_name}` does not exist.")
            return False

    def create_partitioned_table(self) -> None:
        """Creates a new table with a custom schema and time-based partitioning."""
        schema = [
            bigquery.SchemaField(
                "_id",
                "STRING",
                mode="REQUIRED",
                description="Unique ID call made to Youtube API"
            ),
            bigquery.SchemaField(
                "created_at",
                "TIMESTAMP",
                mode="REQUIRED",
                description="Timestamp of the API call"
            ),
            bigquery.SchemaField(
                "username",
                "STRING",
                description="ID of the channel"
            ),
            bigquery.SchemaField(
                "is_username_found",
                "BOOL",
                mode="REQUIRED",
                description="checks if username exists."
            ),
            bigquery.SchemaField(
                "response",
                "STRING",
                mode="REQUIRED",
                description="RAW response.text from YouTube_API"
            ),
            bigquery.SchemaField(
                "response_status_code",
                "INTEGER",
                mode="REQUIRED",
                description="Status code of the API response"
            ),
            bigquery.SchemaField(
                "is_success",
                "BOOL",
                mode="REQUIRED",
                description="Indicates success if response_status_code is 200"
            ),


            bigquery.SchemaField(
                "channel_type",
                "STRING",
                description="Type of the channel"
            ),
            bigquery.SchemaField(
                "channel_id",
                "STRING",
                description="ID of the channel"
            ),
            bigquery.SchemaField(
                "channel_description",
                "STRING",
                description="Description of the channel"
            ),
            bigquery.SchemaField(
                "channel_created_at",
                "STRING",
                description="Creation timestamp of the channel"
            ),
            bigquery.SchemaField(
                "channel_logo",
                "STRING",
                description="URL of the channel's logo"
            ),
            bigquery.SchemaField(
                "channel_country",
                "STRING",
                description="Country of the channel"
            ),
            bigquery.SchemaField(
                "channel_view_count",
                "INTEGER",
                description="Total view count of the channel"
            ),
            bigquery.SchemaField(
                "channel_subscriber_count",
                "INTEGER",
                description="Total subscriber count of the channel"
            ),
            bigquery.SchemaField(
                "is_hidden_subscriber",
                "BOOLEAN",
                description="Whether the subscriber count is hidden"
            ),
            bigquery.SchemaField(
                "channel_video_count",
                "INTEGER",
                description="Total video count of the channel"
            ),
            bigquery.SchemaField(
                "privacy_channel_type",
                "STRING",
                description="Privacy status of the channel"
            ),
            bigquery.SchemaField(
                "is_channel_linked",
                "BOOLEAN",
                description="Whether the channel is linked"
            ),
            bigquery.SchemaField(
                "long_upload_allowed",
                "STRING",
                description="Long upload status"
            ),
            bigquery.SchemaField(
                "is_kid_safe",
                "BOOLEAN",
                description="Whether the channel is marked as kid-safe"
            )
        ]

        table = bigquery.Table(self._table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at",
        )
        table = self._client.create_table(table)
        print(f"Created table {self._table_id}, partitioned on column {table.time_partitioning.field}.")

    def insert_data(self, rows_to_insert: list[dict]) -> None:
        """Inserts data into the table."""
        sleep(10)
        errors = self._client.insert_rows_json(self._table_id, rows_to_insert)
        if not errors:
            print("New rows have been added.")
            # yt_logger.info("Data inserted into BigQuery successfully!")
        else:
            print("Encountered errors while inserting rows: %s", errors)
            # yt_logger.error("Encountered errors while inserting rows: %s", errors)
