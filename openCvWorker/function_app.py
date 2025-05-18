import azure.functions as func
import logging
import os
import tempfile
from openCvHelper import analyze_video

app = func.FunctionApp()

# @app.function_name(name="HttpTrigger")
# @app.route(route="hello")
# def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
#     name = req.params.get("name", "World")
#     return func.HttpResponse(f"Hello, {name}!", status_code=200)

"""
Azure function to process with open CV
Triggers when an mp4 file is uploaded to "segment-container" blob storage
"""
@app.function_name(name="BlobTrigger")
@app.blob_trigger(arg_name="segmentBlob", 
                  path="segment-container/{segmentName}.mp4",
                  connection="AzureWebJobsStorage") # loaded from local.settings.json
def blob_trigger_opencv_processing_function(segmentBlob: func.InputStream):
    
    # Log some basic info 
    file_name = segmentBlob.name
    size = segmentBlob.length
    logging.info(f"Blob trigger fired for: {file_name}, size: {size} bytes")

    # Saving a copy of the video segment locally
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
        tmp.write(segmentBlob.read())
        save_path = tmp.name  # Full safe path like /tmp/tmpx83k9sd3.mp4
    
    if not os.path.exists(save_path):
        logging.error(f"Error video not saved successfully!")
        return
    logging.info(f"File saved successfully {save_path}") 
          
    # Analyze video contents with open CV
    output = analyze_video() # UNDER CONSTRUCTION

    # Delete temporary video file
    os.remove(save_path)
    logging.info("Temp file deleted")