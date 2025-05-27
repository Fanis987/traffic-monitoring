from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

# Set up basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alert-logger")

#Standard route
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
        entered = data["Entered"]
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
            entered = vehicle["Entered"]
            speed = vehicle["speed"]
            vehicle_type = vehicle["Vehicle"]
        except KeyError as e:
            return jsonify({"error": f"Missing field in one of the list items: {e}"}), 400

        logger.warning(f"🚨 ALERT: Vehicle ID {vehicle_id} ({vehicle_type}) was spotted at: {time} (relative time) speeding at {speed} km/h!")

    return jsonify({"status": "received", "count": len(data)})
