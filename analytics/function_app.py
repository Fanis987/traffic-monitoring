import azure.functions as func
from azure.storage.blob import BlobServiceClient
import datetime
import json
import logging
import csv
import tempfile
import pyodbc

app = func.FunctionApp()

@app.route(route="AnalyticsApp", auth_level=func.AuthLevel.FUNCTION)
def AnalyticsApp(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    return func.HttpResponse(
             "All OK",
             status_code=200
        )

    
        