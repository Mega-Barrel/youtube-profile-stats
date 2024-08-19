""" Big Query DB for handling table CURD operations"""

# System level imports
import os
from time import sleep

# Package level imports
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
# Exceptions import
from google.cloud.exceptions import NotFound

def create_partitioned_table() -> None:
    """Creates a new table in the specified dataset, with custom schema and Column Partition.

    Output:
        Creates BQ table with custom schema
    """
    schema = [
        bigquery.SchemaField(
            "_id", 
            field_type="STRING",
            description="UNIQUE _id for every API request"
        ),
        bigquery.SchemaField(
            "created_at", 
            field_type="TIMESTAMP", 
            description="record inserted at"
        ),
        bigquery.SchemaField(
            "response", 
            field_type="JSON", 
            description="RAW JSON payload for YT API"
        ),
        bigquery.SchemaField(
            "response_status_code", 
            field_type="INTEGER", 
            description="status code for YT API"
        ),
        bigquery.SchemaField(
            "is_success", 
            field_type="boolean", 
            description="Boolean field to indicate, if request to YT API was success."
        )
    ]

    # table = bigquery.Table(_table_id, schema=schema)
    # table.time_partitioning = bigquery.TimePartitioning(
    #     type_= bigquery.TimePartitioningType.DAY,
    #     field = "created_at",  # name of column to use for partitioning
    # )

    # table = _client.create_table(table)

    # print(
    #     f"Created table {_table_id}, "
    #     f"partitioned on column {table.time_partitioning.field}."
    # )

