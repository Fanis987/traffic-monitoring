import azure.functions as func
from azure.storage.blob import BlobServiceClient
import datetime
import json
import logging
import csv
import tempfile
import pyodbc
import os

# Queries
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
    conn_str = os.getenv("SQL_STORAGE_CONN_STR")
    error_msg = "Worker could not find the connection string to the Azure SQL Storage. Terminating...."
    if(conn_str == None):
        return func.HttpResponse(error_msg,status_code=500)
    
    # Query the SQL Storage
    (was_successful,rows,output_msg) = query_SQL_storage(conn_str,SELECT_ALL_QUERY)
    if( not was_successful):
        return func.HttpResponse(output_msg,status_code=500)

    return func.HttpResponse(
             f"All OK, found {len(rows)}",
             status_code=200
        )

# Helper function for the writing
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
        logging.e(f"Connection Error: {e}")
        return (False,None,"Connection Error: {e}")
        