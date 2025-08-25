from azure.storage.blob import BlobServiceClient
import os

# Replace with your actual connection string
connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "raw"
local_file_path = "AAPL_prices.csv"
blob_name = "AAPL_prices.csv"

blob_service_client = BlobServiceClient.from_connection_string(connect_str)
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

with open(local_file_path, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

print(f"Uploaded {blob_name} to Azure container '{container_name}'.")