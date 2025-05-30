import azure.functions as func
import datetime
import json
import logging

app = func.FunctionApp()


# Blob trigger that waits for csv files from csv workers
@app.blob_trigger(arg_name="uploadedFile", 
                  path="intermediate-results/{name}.csv", #non-csv filtered out
                  connection="AzureWebJobsStorage") 
def BlobTriggerFunc(uploadedFile: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {uploadedFile.name}"
                f"Blob Size: {uploadedFile.length} bytes")
    
