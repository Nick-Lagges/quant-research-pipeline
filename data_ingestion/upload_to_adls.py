from azure.storage.blob import BlobServiceClient
import os

def upload_to_adls(tickers, connect_str, container_name="raw"):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    for ticker in tickers:
        local_file = f"data_ingestion/sp500/{ticker}_prices.csv"
        blob_name = f"sp500/{ticker}_prices.csv"
        if os.path.exists(local_file):
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            with open(local_file, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            print(f"☁️ Uploaded {ticker} to Azure.")
