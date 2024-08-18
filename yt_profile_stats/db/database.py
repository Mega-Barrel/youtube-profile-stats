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
            "page_id", 
            field_type="STRING",
            description="Notion Page ID"
        ),
        bigquery.SchemaField(
            "created_at", 
            "TIMESTAMP", 
            description="Question CreatedAt"
        )
    ]

    table = bigquery.Table(_table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_= bigquery.TimePartitioningType.DAY,
        field = "created_at",  # name of column to use for partitioning
    )

    table = _client.create_table(table)

    print(
        f"Created table {_table_id}, "
        f"partitioned on column {table.time_partitioning.field}."
    )

