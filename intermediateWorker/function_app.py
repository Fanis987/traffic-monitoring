import azure.functions as func
import datetime
import json
import logging
import csv
import io

app = func.FunctionApp()


# Blob trigger that waits for csv files from csv workers
@app.blob_trigger(arg_name="uploadedFile", 
                  path="intermediate-results/{name}.csv", #non-csv filtered out
                  connection="AzureWebJobsStorage") 
def BlobTriggerFunc(uploadedFile: func.InputStream):
    
    # Parsing the uploaded CSV file
    try:
        blob_text = uploadedFile.read().decode('utf-8')
        vehicle_records_list = []

        # read each line into nonymous object and store it
        reader = csv.DictReader(io.StringIO(blob_text))
        for row in reader:
            record = {
            'timeEntered': float(row['timeEntered']),
            'speed': float(row['speed']),
            'vehicleType': row['vehicleType'],
            'lane': row['lane'],
            'speeding': int(row['speeding'])}
            vehicle_records_list.append(record)

        vehicle_records_num : int = len(vehicle_records_list)
        if(vehicle_records_num == 0):
            logging.error(f"The uploaded file had no info on vehicles ")
            return
        logging.info(f"Found {vehicle_records_num} vehicle records")
    
    except KeyError:
        logging.error(f"The uploaded csv file had incorrect headers ")
        return
    
    except Exception as e:
        logging.error(f"Exception occured: {e} ")
        return
    
    
