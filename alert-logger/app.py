from flask import Flask, request, jsonify
import logging
import sys

app = Flask(__name__)

# Configure logger to output to stdout for Azure logging
logger = logging.getLogger("alert-logger")
logger.setLevel(logging.WARNING)

if logger.hasHandlers():
    logger.handlers.clear()

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.WARNING)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)

# Standard route
@app.route("/")
def hello():
    return "Hello, This is the alert key logger app!"


@app.route("/api/alert", methods=["POST"])
def receive_alert():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid or missing JSON"}), 400

    try:
        vehicle_id = data["ID"]
        time = data["Time"]
        speed = data["speed"]
        vehicle_type = data["Vehicle"]
    except KeyError as e:
        return jsonify({"error": f"Missing field: {e}"}), 400

    logger.warning(f"🚨 ALERT: Vehicle ID {vehicle_id} ({vehicle_type}) was spotted at: {time} (relative time) speeding at {speed} km/h!")

    return jsonify({"status": "received"})


@app.route("/api/alert/list", methods=["POST"])
def receive_alerts():
    data = request.get_json()
    if not data or not isinstance(data, list):
        return jsonify({"error": "Invalid or missing JSON list"}), 400

    for vehicle in data:
        try:
            vehicle_id = vehicle["ID"]
            time = vehicle["Time"]
            speed = vehicle["speed"]
            vehicle_type = vehicle["Vehicle"]
        except KeyError as e:
            return jsonify({"error": f"Missing field in one of the list items: {e}"}), 400

        logger.warning(f"🚨 ALERT: Vehicle ID {vehicle_id} ({vehicle_type}) was spotted at: {time} (relative time) speeding at {speed} km/h!")

    return jsonify({"status": "received", "count": len(data)})
