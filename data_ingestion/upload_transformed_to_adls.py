from azure.storage.blob import BlobServiceClient
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()
connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

def upload_transformed(date_str, connect_str, container_name="transformed"):
    tickers_file = f"sp500_tickers_{date_str}.csv"
    tickers = pd.read_csv(tickers_file)['Cleaned'].tolist()

    current_path = os.path.dirname(__file__)
    parent_dir = os.path.dirname(current_path)

    local_folder = f"{parent_dir}/transformed/{date_str}/"

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    with open("logs/upload_transformed_log.txt", "a") as log:
        for ticker in tickers:
            local_file = f"{local_folder}{ticker}_transformed.csv"
            blob_name = f"transformed/{date_str}/{ticker}_transformed.csv"

            if os.path.exists(local_file):
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                with open(local_file, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                print(f"☁️ Uploaded transformed {ticker}")
                log.write(f"{datetime.now()} | {ticker} | UPLOADED\n")
            else:
                print(f"❌ Missing transformed file for {ticker}")
                log.write(f"{datetime.now()} | {ticker} | MISSING\n")

if __name__ == "__main__":
    date_str = sorted(os.listdir("sp500"))[-1]
    if not connect_str:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found in environment.")
    upload_transformed(date_str, connect_str)