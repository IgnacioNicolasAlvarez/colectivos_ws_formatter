import json
import logging
import os
from datetime import datetime, timedelta

import azure.functions as func
import polars as pl
from azure.storage.blob import BlobServiceClient
from deltalake import write_deltalake

from helpers import flatten_data

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 1 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def timer_compact_json(myTimer: func.TimerRequest) -> None:
    FROM_CONTAINER = "bronze"
    STORAGE_OPTIONS = {
        "ACCOUNT_NAME": os.getenv("storageaccountina_STORAGE_ACCOUNTNAME"),
        "ACCESS_KEY": os.getenv("storageaccountina_STORAGE_ACCESSKEY"),
    }

    STORAGE_CONTAINERNAME = os.getenv("storageaccountina_STORAGE_CONTAINERNAME")

    yesterday = datetime.now() - timedelta(days=1)
    path = f"posiciones/{yesterday.year}/{yesterday.month}/{yesterday.day}"

    service_client = BlobServiceClient.from_connection_string(
        os.getenv("storageaccountina_STORAGE")
    )

    container_client = service_client.get_container_client(FROM_CONTAINER)
    blobs = container_client.list_blobs(name_starts_with=path)

    all_data = []
    blob_count = 0

    for b in blobs:
        name = b.name
        if not name.endswith(".json"):
            continue
        blob_client = container_client.get_blob_client(name)
        raw = blob_client.download_blob().readall().decode("utf-8")
        if raw:
            data = json.loads(raw)
            flattened_data = flatten_data(data)

            for item in flattened_data:
                item["insertion_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                if "reported_at" in item:
                    try:
                        dt = datetime.strptime(
                            item["reported_at"], "%d/%m/%Y - %H:%M:%S"
                        )
                        item["reported_at"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        logging.warning(
                            f"Invalid date format in record: {item['reported_at']}"
                        )

            all_data.extend(flattened_data)
            blob_count += 1

            if blob_count >= 5000:
                df = pl.DataFrame(all_data)
                df = df.with_columns(add_date_columns())

                write(STORAGE_OPTIONS, STORAGE_CONTAINERNAME, df)
                all_data = []
                blob_count = 0

    if all_data:
        df = pl.DataFrame(all_data)
        df = df.with_columns([add_date_columns()])

        write(STORAGE_OPTIONS, STORAGE_CONTAINERNAME, df)


def write(STORAGE_OPTIONS, STORAGE_CONTAINERNAME, df):
    write_deltalake(
        table_or_uri=f"abfs://{STORAGE_CONTAINERNAME}/posiciones",
        data=df,
        storage_options=STORAGE_OPTIONS,
        partition_by=["year", "month", "day"],
        mode="append",
    )


def add_date_columns():
    return [
        pl.col("reported_at").str.strptime(pl.Datetime).dt.year().alias("year"),
        pl.col("reported_at").str.strptime(pl.Datetime).dt.month().alias("month"),
        pl.col("reported_at").str.strptime(pl.Datetime).dt.day().alias("day"),
    ]
