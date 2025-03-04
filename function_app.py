import json
import os

import azure.functions as func
import azurefunctions.extensions.bindings.blob as blob
import polars as pl

from helpers import flatten_data

app = func.FunctionApp()


@app.blob_trigger(
    arg_name="client", path="bronze", connection="storageaccountina_STORAGE"
)
def blob_trigger(client: blob.BlobClient):
    name = client.name
    if not name.endswith('.json'):
        return
        
    raw = client.download_blob().readall().decode("utf-8")
    if raw:
        data = json.loads(raw)
    else:
        return

    df = pl.DataFrame(flatten_data(data))

    container = os.getenv("storageaccountina_STORAGE_CONTAINERNAME")

    storage_options = {
        "ACCOUNT_NAME": os.getenv("storageaccountina_STORAGE_ACCOUNTNAME"),
        "ACCESS_KEY": os.getenv("storageaccountina_STORAGE_ACCESSKEY"),
    }

    df.write_delta(
        f"abfs://{container}/posiciones",
        storage_options = storage_options,
        mode="append"
    )