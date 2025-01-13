import os
import json
import asyncio
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.iot.device import IoTHubModuleClient
from azure.core.exceptions import ResourceExistsError

#load_dotenv("/config/config.env")
CAM_ID = "cam-02" # os.environ.get("CAMERA_NAME")
#BLOB_MODULE = "192.168.29.245"
BLOB_MODULE = "storage_module"
STORAGE_ACCOUNT_NAME = "edgestore"
STORAGE_ACCOUNT_KEY = "7Cyf5F4Arlo96IKsm+eWsw=="
CONNECTION_STRING = f"DefaultEndpointsProtocol=http;BlobEndpoint=http://{BLOB_MODULE}:11002/{STORAGE_ACCOUNT_NAME};AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};"
print(f"Connecting to Edge blob account {STORAGE_ACCOUNT_NAME} with {CONNECTION_STRING}")
INPUT_FOLDER = "/store"
try:
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING, api_version="2019-12-12")
    print("Blob service client created")
except Exception as e:
    print(f"Blob service client creation error: {e}")

# Create a unique name for the container
image_container_name = f"{CAM_ID}-imagestore"
video_container_name = f"{CAM_ID}-videostore"

try:
    # Create image container
    container_client = blob_service_client.create_container(image_container_name)
    print(f"Container {image_container_name} created")
except ResourceExistsError:
    print(f"A container with name {image_container_name} already exists")
    pass

try:
    # Create video container
    container_client = blob_service_client.create_container(video_container_name)
    print(f"Container {video_container_name} created")
except ResourceExistsError:
    print(f"A container with name {video_container_name} already exists")
    pass 

async def handle_message(message):
    try:
        message_text = message.data.decode("utf-8")
        message_json = json.loads(message_text)
        print(message_json)
        if message.input_name == "image_blob_trigger":
            file_name = message_json["alert_image"]
            alert_id = message_json["alert_id"],
            frame_id = message_json["frame_id"]
            cam_id = message_json["cam_id"]
            alert_day = message_json["alert_day"]
            blob_name = f"{alert_id}-{cam_id}-{alert_day}-{frame_id}.jpg"

            try:
                # Create a blob client using the local file name as the name for the blob
                blob_client = blob_service_client.get_blob_client(container=image_container_name, blob=blob_name)
            except Exception as e:
                print(f"Blob client creations error: {e}")

        elif message.input_name == "video_blob_trigger":
            file_name = message_json["alert_video"]
            alert_id = message_json["alert_id"]
            cam_id = message_json["cam_id"]
            alert_day = message_json["alert_day"]
            blob_name = f"{alert_id}-{cam_id}-{alert_day}.avi"
            
            try:
                # Create a blob client using the local file name as the name for the blob
                blob_client = blob_service_client.get_blob_client(container=video_container_name, blob=blob_name)
            except Exception as e:
                print(f"Blob client creations error: {e}")

        else:
            print("Unknown Input Stream")

        # Upload the created file
        if file_name:
            print(f"Uploading to Azure Cloud Storage as blob: {file_name}")
            with open(file=file_name, mode="rb") as data:
                await blob_client.upload_blob(data, overwrite=True)
                print(f"{file_name} added to container")

    except Exception as e:
        print(f"Error processing input message: {e}")

async def main():


    #connection_string = "HostName=WaterUtilityHub.azure-devices.net;DeviceId=dot-edge-vision2;SharedAccessKey=BYNmMKdeQUscnzAFRHPciWX1A1I7cfGHcNRW3zBvDfw="
    try:
        module_client = IoTHubModuleClient.create_from_edge_environment()
        #module_client = IoTHubModuleClient.create_from_connection_string(connection_string)
        module_client.connect()
    except Exception as e:
        print(f"Module Connection Error: {e}")

    module_client.on_message_received = handle_message

    while True:
        await asyncio.sleep(100)


if __name__=="__main__":
    asyncio.run(main())