import logging
import os
import tempfile
from azure.storage.blob import BlobServiceClient
import azure.functions as func
from proccess2 import analyse_clip

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="opecv_http_trigger")
def opecv_http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    filename = req.params.get("filename")
    if not filename:
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = {}
        filename = req_body.get("filename")

    if not filename:
        return func.HttpResponse(
            "Please pass the filename in the query string or in the request body",
            status_code=400
        )

    try:
        # Connect to blob storage
        conn_str = os.getenv("AzureWebJobsStorage")
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service_client.get_container_client("output-segments")

        # Download the specified file
        blob_client = container_client.get_blob_client(filename)
        video_bytes = blob_client.download_blob().readall()

        # Save to temp file
        temp_dir = tempfile.gettempdir()
        video_path = os.path.join(temp_dir, filename)
        with open(video_path, "wb") as f:
            f.write(video_bytes)

        logging.info(f"Video saved locally to: {video_path}")

        # Prepare CSV output path
        csv_name = os.path.splitext(filename)[0] + ".csv"
        csv_output_path = os.path.join(temp_dir, csv_name)

        # Run your analysis
        analyse_clip(video_path, csv_output_path, show_video=False)
        logging.info(f"CSV generated: {csv_output_path}")

        # Upload result to Intermediate-results
        output_container = blob_service_client.get_container_client("Intermediate-results")
        with open(csv_output_path, "rb") as data:
            output_container.upload_blob(name=csv_name, data=data, overwrite=True)
        logging.info(f"CSV uploaded as: {csv_name}")

        return func.HttpResponse(f"Success: Processed and uploaded {csv_name}", status_code=200)

    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"Error processing file: {str(e)}", status_code=500)

    finally:
        # Cleanup temp files
        try:
            os.remove(video_path)
            os.remove(csv_output_path)
        except Exception as cleanup_err:
            logging.warning(f"Cleanup failed: {cleanup_err}")