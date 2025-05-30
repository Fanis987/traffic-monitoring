import azure.functions as func
import datetime
import json
import logging
import csv
import io
import pyodbc
import os

app = func.FunctionApp()


# Blob trigger that waits for csv files from csv workers and processes them
@app.blob_trigger(arg_name="uploadedFile", 
                  path="intermediate-results/{name}.csv", #non-csv filtered out
                  connection="AzureWebJobsStorage") 
def BlobTriggerFunc(uploadedFile: func.InputStream):
    
    try:
        # Get Connnction String to SQL Storage
        SQL_STORAGE_CONN_STRING = os.getenv("SQL_STORAGE_CONN_STRING")
        if(SQL_STORAGE_CONN_STRING == None):
            logging.error(f"The connection string to Azure SQL Storage could not be found ")
            return

        # Parsing the uploaded CSV file
        blob_text = uploadedFile.read().decode('utf-8')
        vehicle_records_list = []

        # Read each csv line into TUPLE and store it to list
        reader = csv.DictReader(io.StringIO(blob_text))
        for row in reader:
            record = (
            int(row['vehicleId']),
            float(row['timeEntered']),
            float(row['speed']),
            row['vehicleType'],
            row['lane'],
            int(row['speeding']))
            vehicle_records_list.append(record)

        # Checking the number of records found
        vehicle_records_num : int = len(vehicle_records_list)
        if(vehicle_records_num == 0):
            logging.error(f"The uploaded file had no info on vehicles ")
            return
        logging.info(f"Found {vehicle_records_num} vehicle records")

        # Try to send the extracted data to Azure SQL storage
        (upload_suceeded,error_msg) = save_data_to_SQL_storage(SQL_STORAGE_CONN_STRING, vehicle_records_list)
        if(not upload_suceeded):
            logging.error(f"Could not upload data to Azure storage: {error_msg}")
            return
        logging.info(f"Added {vehicle_records_num} rows to SQL Storage Table")

    
    except KeyError:
        logging.error(f"The uploaded csv file had incorrect headers ")
        return
    
    except Exception as e:
        logging.error(f"Exception occured: {e} ")
        return
    

# Helper function for writing data to Azure SQL storage
def save_data_to_SQL_storage(conn_str: str, data_rows: list ) -> tuple[bool, str]:
     
    insert_query = """
        INSERT INTO vehicledata (vehicleId, timeEntered, speed, vehicletype, lane, speeding)
        VALUES (?, ?, ?, ?, ?, ?)
    """

    try:
        # TRy coonnecting with Azure Storage via ODBC
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.fast_executemany = True  # improves execute many performance
                cursor.executemany(insert_query, data_rows)
                
                conn.commit() # BATCH commit
                return (True,"")
                
    except Exception as e:
        return (False,f"Connection Error: {e}")
    
