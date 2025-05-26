from fastapi import FastAPI, Request
from pydantic import BaseModel
import logging
from typing import List

app = FastAPI()

# Set up basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alert-logger")

# Define the expected structure of the incoming JSON
class VehicleData(BaseModel):
    ID: int
    Time: float
    Entered: bool
    speed: float
    Vehicle: str

# POST endpoint to receive alert data
@app.post("/api/alert")
async def receive_alert(data: VehicleData):
    logger.warning(f"🚨 ALERT: Vehicle ID {data.ID} ({data.Vehicle}) was spotted at: {data.Time} (relative time) speeding at {data.speed} km/h!")

    return {"status": "received"}

# POST endpoint to receive alert data (list of vehicles)
@app.post("/api/alert/list")
async def receive_alerts(data: List[VehicleData]):
    for vehicle in data:
        logger.warning(f"🚨 ALERT: Vehicle ID {vehicle.ID} ({vehicle.Vehicle}) was spotted at: {vehicle.Time} (relative time) speeding at {vehicle.speed} km/h!")
    
    return {"status": "received", "count": len(data)}