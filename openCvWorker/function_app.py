import azure.functions as func
import logging

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
    print("test")

    # Using Content with open CV
    #content : bytes = segmentBlob.read() #bytes of the file uploaded