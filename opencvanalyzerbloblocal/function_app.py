import logging
import os
import tempfile
from azure.storage.blob import BlobServiceClient
import azure.functions as func
from proccess2 import analyse_clip 

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="output-segments/{name}",
                               connection="auebprojectvideo_STORAGE") 
def open_cv_analyzer(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

     # Save video blob to temporary file
    temp_dir = tempfile.gettempdir()
    video_path = os.path.join(temp_dir, myblob.name)

    temp_path = os.path.join(os.getenv("TEMP", "/tmp"), myblob.name)

    with open(temp_path, "wb") as f:
        f.write(myblob.read())

    logging.info(f"Video saved locally to: {temp_path}")

    # Output CSV path
    csv_name = os.path.splitext(myblob.name)[0] + ".csv"
    csv_output_path = os.path.join(temp_dir, csv_name)

    try:
        analyse_clip(temp_path, csv_output_path, show_video=False)
        logging.info(f"CSV generated: {csv_output_path}")

        # Upload CSV to output-csv container
        conn_str = os.getenv("AzureWebJobsStorage")
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service_client.get_container_client("output-csv")
        with open(csv_output_path, "rb") as data:
            container_client.upload_blob(name=csv_name, data=data, overwrite=True)

        logging.info(f"CSV uploaded to 'output-csv' container as '{csv_name}'")

    except Exception as e:
        logging.error(f"Error during analysis or upload: {e}")
    finally:
        # Clean up temp files
        try:
            os.remove(temp_path)
            os.remove(csv_output_path)
        except Exception as cleanup_err:
            logging.warning(f"Cleanup failed: {cleanup_err}")