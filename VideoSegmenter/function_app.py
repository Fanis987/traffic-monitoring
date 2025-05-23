import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from moviepy import VideoFileClip
from pathlib import Path
import tempfile
import os

CONNECT_STR = os.getenv('AzureWebJobsStorage')
INPUT_CONTAINER = "input-video"
OUTPUT_CONTAINER = "output-segments"

app = func.FunctionApp()

def download_blob_to_temp(blob_service_client, container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    with open(temp_file.name, "wb") as f:
        f.write(blob_client.download_blob().readall())
    return temp_file.name

def upload_blob(blob_service_client, container_name, blob_name, file_path):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    with open(file_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

@app.route(route="VideoSegmentFunction", auth_level=func.AuthLevel.FUNCTION)
def VideoSegmentFunction(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        video_name = req.params.get("video")
        if not video_name:
            return func.HttpResponse("Please pass a 'video' name in the query string.", status_code=400)

        blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)
        local_video_path = download_blob_to_temp(blob_service_client, INPUT_CONTAINER, video_name)

        main_clip = VideoFileClip(local_video_path)
        segment_duration = 120  # 2 minutes
        video_duration = main_clip.duration
        num_full_segments = int(video_duration // segment_duration)

        for i in range(num_full_segments):
            start = i * segment_duration
            end = start + segment_duration
            subclip = main_clip.subclipped(start, end).without_audio()
            with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as temp_output:
                subclip.write_videofile(temp_output.name, codec="libx264", audio_codec="aac")
                output_blob_name = f"{Path(video_name).stem}_part{i+1}.mp4"
                upload_blob(blob_service_client, OUTPUT_CONTAINER, output_blob_name, temp_output.name)

        # Handle leftover segment
        remaining_time = video_duration - num_full_segments * segment_duration
        if remaining_time > 0:
            start = num_full_segments * segment_duration
            end = video_duration
            subclip = main_clip.subclipped(start, end).without_audio()
            with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as temp_output:
                subclip.write_videofile(temp_output.name, codec="libx264", audio_codec="aac")
                output_blob_name = f"{Path(video_name).stem}_part{num_full_segments+1}.mp4"
                upload_blob(blob_service_client, OUTPUT_CONTAINER, output_blob_name, temp_output.name)

        return func.HttpResponse(f"Video '{video_name}' segmented successfully.", status_code=200)

    except Exception as e:
        logging.exception("Error during video segmentation")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)