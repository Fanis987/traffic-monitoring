import azure.functions as func
from azure.storage.blob import BlobServiceClient
import datetime
import json
import logging
import csv
import tempfile
import pyodbc
import os

# Queries Consts
SELECT_ALL_QUERY = """
        SELECT * FROM VehicleData
    """

VEHICLES_PER_LANE_QUERY = """
        SELECT lane, COUNT(*) AS count_per_lane
        FROM VehicleData
        GROUP BY lane
        ORDER BY lane;
    """

SPEEDING_TOTAL_QUERY = """
        SELECT COUNT(*) AS speeding_count
        FROM VehicleData
        WHERE speeding = 1
    """

VEHICLES_PER_5MIN_PER_LANE= """
        SELECT FLOOR(TimeEntered / 300) AS five_min_bucket, lane,  COUNT(*) AS count_per_bucket
        FROM VehicleData
        GROUP BY lane, FLOOR(TimeEntered / 300)
        ORDER BY five_min_bucket, lane;
    """

AVG_SPD_PER_LANE_PER_5MIN_QUERY = """
        SELECT lane, FLOOR(TimeEntered / 300) AS time_bucket, ROUND(AVG(speed), 1) AS avg_speed_kmh
        FROM VehicleData
        GROUP BY lane, FLOOR(TimeEntered / 300)
        ORDER BY lane, time_bucket , avg_speed_kmh;
    """

app = func.FunctionApp()

# Test Query: Get all data from SQL storage
@app.route(route="Q0", auth_level=func.AuthLevel.FUNCTION)
def AnalyticsApp(req: func.HttpRequest) -> func.HttpResponse:
    
    # Get necessary env variables
    SQL_STORAGE_CONN_STR = os.getenv("SQL_STORAGE_CONN_STR")
    error_msg = "Analyzing function could not find the connection string to the Azure SQL Storage. Terminating...."
    if(SQL_STORAGE_CONN_STR == None):
        return func.HttpResponse(error_msg,status_code=500)
    
    BLOB_CONNECT_STR = os.getenv('AzureWebJobsStorage')
    error_msg = "Analyzing function could not find the connection string to the output Blob Storage. Terminating...."
    if(BLOB_CONNECT_STR == None):
        return func.HttpResponse(error_msg,status_code=500)
    
    BLOB_CONTAINER_NAME= os.getenv('BLOB_CONTAINER_NAME')
    error_msg = "Analyzing function could not find the name of the output Blob Storage Container. Terminating...."
    if(BLOB_CONTAINER_NAME == None):
        return func.HttpResponse(error_msg,status_code=500)
    
    # Query the SQL Storage
    (was_successful,rows,output_msg) = query_SQL_storage(SQL_STORAGE_CONN_STR,VEHICLES_PER_LANE_QUERY)
    if( not was_successful):
        return func.HttpResponse(output_msg,status_code=500)
    
    # Upload result of query to Azure Blob Storage
    (upload_successful, error_msg) = save_csv_and_upload(rows, BLOB_CONNECT_STR, BLOB_CONTAINER_NAME, "result-query")
    if( not upload_successful):
        return func.HttpResponse(error_msg,status_code=500)

    return func.HttpResponse("query executed successfully",status_code=200)


# Helper function for the querying the SQL storage
def query_SQL_storage(conn_str: str, query: str) -> tuple[bool, list, str]:
    
    try:
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                # execute the requested query
                cursor.execute(query)
                rows = cursor.fetchall()
                                
                if(len(rows) == 0):
                    logging.info(f"No records found by the query")
                    return (False,None,"No records found by the query")
                
                # return the query results
                logging.info(f"Found {len(rows)} records with the query")
                return (True,rows,"")
                
    except Exception as e:
        logging.error(f"Connection Error: {e}")
        return (False,None,f"Connection Error: {e}")


# Helper function to save the query results to a local csv and upload them to blob storage  
def save_csv_and_upload(rows, blob_conn_str, container_name, blob_output_filename)-> tuple[bool, str]:
    
    try:
        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix=".csv") as temp_output:  
            writer = csv.writer(temp_output)
            for row in rows:
                writer.writerow(row)
            temp_file_path = temp_output.name

        # Upload to Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_output_filename)
        with open(temp_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        # Cleanup local file
        os.remove(temp_file_path)
        return (True,"")
    
    except Exception as e:
        logging.error(f"Error occured: {e}")
        return (False,f"Error occured: {e}")    