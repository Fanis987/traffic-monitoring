import azure.functions as func
import logging
import csv
import io
import pyodbc
import os
import requests
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

@app.function_name(name="HttpTriggerFunc")
@app.route(route="process", auth_level=func.AuthLevel.ANONYMOUS)
def HttpTriggerFunc(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP trigger function to process a blob file by filename.')

    # Extract filename
    filename = req.params.get('filename')
    if not filename:
        try:
            req_body = req.get_json()
            filename = req_body.get('filename')
        except ValueError:
            pass

    if not filename:
        return func.HttpResponse("Please pass a 'filename' parameter in the query or body.", status_code=400)

    # Environment variables
    ALERT_WEB_APP_URL = os.getenv("ALERT_WEB_APP_URL")
    SQL_STORAGE_CONN_STRING = os.getenv("SQL_STORAGE_CONN_STRING")
    BLOB_CONN_STRING = os.getenv("AzureWebJobsStorage")
    BLOB_CONTAINER_NAME = "intermediate-results"

    if not ALERT_WEB_APP_URL or not SQL_STORAGE_CONN_STRING or not BLOB_CONN_STRING:
        return func.HttpResponse("Missing required environment variables.", status_code=500)

    try:
        # Connect to blob storage and download the file
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STRING)
        blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=filename)

        if not blob_client.exists():
            return func.HttpResponse(f"Blob file '{filename}' not found in container.", status_code=404)

        blob_data = blob_client.download_blob().readall().decode('utf-8')

        vehicle_records_list = []
        reader = csv.DictReader(io.StringIO(blob_data))

        for row in reader:
            record = (
                int(row['vehicleId']),
                float(row['timeEntered']),
                float(row['speed']),
                row['vehicleType'],
                row['lane'],
                int(row['speeding'])
            )
            vehicle_records_list.append(record)

        if not vehicle_records_list:
            return func.HttpResponse("CSV file has no records.", status_code=400)

        # Save to SQL
        upload_succeeded, error_msg = save_data_to_SQL_storage(SQL_STORAGE_CONN_STRING, vehicle_records_list)
        if not upload_succeeded:
            return func.HttpResponse(f"SQL error: {error_msg}", status_code=500)

        # Check for speeding vehicles
        speeding_vehicles = [
            {
                "vehicleId": rec[0],
                "timeEntered": rec[1],
                "speed": rec[2],
                "vehicleType": rec[3]
            }
            for rec in vehicle_records_list if rec[2] > 130
        ]

        if speeding_vehicles:
            alert_successful, alert_error = send_alert(ALERT_WEB_APP_URL, speeding_vehicles)
            if not alert_successful:
                return func.HttpResponse(f"Alert failed: {alert_error}", status_code=500)

        return func.HttpResponse(
            f"Successfully processed {len(vehicle_records_list)} records. "
            f"{len(speeding_vehicles)} speeding vehicles found.",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Exception occurred: {e}")
        return func.HttpResponse(f"Internal error: {e}", status_code=500)


# Helper functions remain exactly the same as in your original code
def save_data_to_SQL_storage(conn_str: str, data_rows: list) -> tuple[bool, str]:
    insert_query = """
        INSERT INTO vehicledata (vehicleId, timeEntered, speed, vehicletype, lane, speeding)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    try:
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.fast_executemany = True
                cursor.executemany(insert_query, data_rows)
                conn.commit()
                return (True, "")
    except Exception as e:
        return (False, f"Connection Error: {e}")

def send_alert(app_url: str, alert_data) -> tuple[bool, str]:
    try:
        headers = {"Content-Type": "application/json"}
        logging.info("Sending POST to Alert web app")
        response = requests.post(app_url, json=alert_data, headers=headers)
        logging.info(f"Alert web app responded with code: {response.status_code}\n{response.text}")
        return (True, "")
    except Exception as e:
        return (False, f"Connection Error: {e}")