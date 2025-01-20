import os
import json
import asyncio
import tracemalloc
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.iot.device import IoTHubModuleClient
from azure.core.exceptions import ResourceExistsError

# Enable tracemalloc for better debugging
tracemalloc.start()

# Configuration
CAM_ID = os.environ.get("CAMERA_NAME", "cam-03")
BLOB_MODULE = os.environ.get("BLOB_MODULE", "storage_module")
PORT = os.environ.get("BLOB_PORT", "11002")
STORAGE_ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME", "edgestore")
STORAGE_ACCOUNT_KEY = os.environ.get("STORAGE_ACCOUNT_KEY", "7Cyf5F4Arlo96IKsm+eWsw==")
INPUT_FOLDER = "/store"

# Construct connection string
CONNECTION_STRING = (
    f"DefaultEndpointsProtocol=http;"
    f"BlobEndpoint=http://{BLOB_MODULE}:{PORT}/{STORAGE_ACCOUNT_NAME};"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
)

print(f"Connecting to Edge blob account {STORAGE_ACCOUNT_NAME}")

# Initialize blob service client
try:
    blob_service_client = BlobServiceClient.from_connection_string(
        CONNECTION_STRING, 
        api_version="2019-12-12"
    )
    print("Blob service client created")
except Exception as e:
    print(f"Blob service client creation error: {e}")
    #raise

# Container names
image_container_name = f"{CAM_ID}-imagestore"
video_container_name = f"{CAM_ID}-videostore"

# Create containers if they don't exist
def setup_containers():
    try:
        # Create image container
        blob_service_client.create_container(image_container_name)
        print(f"Container {image_container_name} created")
    except ResourceExistsError:
        print(f"Container {image_container_name} already exists")
    except Exception as e:
        print(f"Error creating image container: {e}")
        pass

    try:
        # Create video container
        blob_service_client.create_container(video_container_name)
        print(f"Container {video_container_name} created")
    except ResourceExistsError:
        print(f"Container {video_container_name} already exists")
    except Exception as e:
        print(f"Error creating video container: {e}")
        pass

def message_handler(message):
    """Synchronous handler for IoT Hub messages"""
    try:
        message_text = message.data.decode("utf-8")
        message_json = json.loads(message_text)
        print(f"Received message: {message_json}")
        
        if message.input_name == "image_blob_trigger":
            file_name = message_json["alert_image"]
            alert_id = message_json["alert_id"]
            frame_id = message_json["frame_id"]
            cam_id = message_json["cam_id"]
            alert_day = message_json["alert_day"]
            blob_name = f"{alert_id}-{cam_id}-{alert_day}-{frame_id}.jpg"
            container_name = image_container_name

        elif message.input_name == "video_blob_trigger":
            file_name = message_json["alert_video"]
            alert_id = message_json["alert_id"]
            cam_id = message_json["cam_id"]
            alert_day = message_json["alert_day"]
            blob_name = f"{alert_id}-{cam_id}-{alert_day}.avi"
            container_name = video_container_name
        else:
            print(f"Unknown Input Stream: {message.input_name}")
            return

        # Create a blob client and upload the file
        try:
            blob_client = blob_service_client.get_blob_client(
                container=container_name, 
                blob=blob_name
            )

            if file_name and os.path.exists(file_name):
                print(f"Uploading to Storage container as blob: {blob_name}")
                with open(file=file_name, mode="rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                print(f"Successfully uploaded {file_name} as {blob_name}")
                print(f"Blob Properties: {blob_client.get_blob_properties()}")
            else:
                print(f"File not found: {file_name}")

        except Exception as e:
            print(f"Error uploading blob: {e}")
            pass

    except Exception as e:
        print(f"Error processing message: {e}")
        # Log the error but don't raise to keep the handler running
        return

async def main():
    """Main application loop"""
    # Set up containers first
    setup_containers()
    
    # Initialize IoT Hub client
    try:
        module_client = IoTHubModuleClient.create_from_edge_environment()
        module_client.connect()
        print("IoT Hub module client connected")
    except Exception as e:
        print(f"Error connecting to IoT Hub: {e}")
        return

    try:
        # Register message handler
        module_client.on_message_received = message_handler
        print("Message handler registered")

        # Keep the application running
        while True:
            await asyncio.sleep(60)  # Health check every minute
            print("Module running - waiting for messages...")

    except Exception as e:
        print(f"Unexpected error in main loop: {e}")
    finally:
        # Clean up resources
        print("Shutting down...")
        await module_client.disconnect()
        await blob_service_client.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Application stopped by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        raise