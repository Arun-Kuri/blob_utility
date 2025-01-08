import json
import asyncio
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.iot.device import IoTHubModuleClient
from azure.core.exceptions import ResourceExistsError

CONNECTION_STRING = "DefaultEndpointsProtocol=http;BlobEndpoint=http://storage_module:11002/edgestore;AccountName=edgestore;AccountKey=iYMKd+VQXJDxNwGInLuzl9tA5oUlxTcZnIVhfcGoUnRkXgGl7CROA7fYjOvXd+qFulujnhqjsdtYZpfNHqAVPg==;"
INPUT_FOLDER = "/store"

account_url = "https://<storageaccountname>.blob.core.windows.net"
default_credential = DefaultAzureCredential()

# Create the BlobServiceClient object
blob_service_client = BlobServiceClient(account_url, credential=default_credential)

# Create a unique name for the container
container_name = "edgestore"

# Create the container
try:
    container_client = blob_service_client.create_container(container_name)
except ResourceExistsError:
    print('A container with this name already exists')
    pass


async def handle_input(message):
    try:
        message_text = message.data.decode("utf-8")
        message_json = json.loads(message_text)
        if message.input_name == "image_blob_trigger":
            file_name = message_json["alert_image"]
            alert_id = message_json["alert_id"],
            frame_id = message_json["frame_id"]
            cam_id = message_json["cam_id"]
            alert_day = message_json["alert_day"]
            blob_name = f"{alert_id}-{cam_id}-{alert_day}-{frame_id}.jpg"
        elif message.input_name == "video_blob_trigger":
            file_name = message_json["alert_video"]
            alert_id = message_json["alert_id"]
            cam_id = message_json["cam_id"]
            alert_day = message_json["alert_day"]
            blob_name = f"{alert_id}-{cam_id}-{alert_day}.avi"
    
        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        print(f"Uploading to Azure Storage as blob: {file_name}")
        
        # Upload the created file
        with open(file=file_name, mode="rb") as data:
            await blob_client.upload_blob(data, overwrite=True)

    except Exception as e:
        print(f"Error processing input message: {e}")

async def main():
    module_client = IoTHubModuleClient.create_from_edge_environment()
    await module_client.connect()
    module_client.on_message_received = handle_input

    while True:
        await asyncio.sleep(100)


if __name__=="__main__":
    asyncio.run(main())