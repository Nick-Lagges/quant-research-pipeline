from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import pandas as pd
import os

def upload_to_adls(recent_tickers, connect_str, container_name="raw"):
    tickers = pd.read_csv(recent_tickers)['Cleaned'].tolist()
    date_str = recent_tickers.split("_")[-1].replace(".csv", "")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    with open("logs/upload_log.txt", "a") as log:
        for ticker in tickers:
            local_file = f"sp500/{date_str}/{ticker}_prices.csv"
            blob_name = f"sp500/{date_str}/{ticker}_prices.csv"
            if os.path.exists(local_file):
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                with open(local_file, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                print(f"☁️ Uploaded {ticker} to Azure.")
                log.write(f"{datetime.now()} | {ticker} | UPLOADED\n")


if __name__ == "__main__":
    load_dotenv()
    upload_to_adls("sp500_tickers_2025-08-25.csv", os.getenv("AZURE_STORAGE_CONNECTION_STRING"))