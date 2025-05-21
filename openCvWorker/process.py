import cv2
import numpy as np
from ultralytics import YOLO
import pandas as pd
import logging

# Constants
LANE_DISTANCE_METERS = 20
FRAME_RATE = 25
MIN_SPEED_KMH = 5
MAX_DISTANCE = 50
SPEED_LIMIT = 120

model = YOLO("yolov8n.pt")

def calibrate_pixels_per_meter():
    return (280 - 140) / LANE_DISTANCE_METERS  # Adjusted upper and lower y-limits

def analyze_video(video_path, output_csv="vehicle_data.csv"):
    logging.info("Analysis started")
    cap = cv2.VideoCapture(video_path)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))

    roi_y = 400
    roi_height = 140
    roi_width = 400

    side_offset = 150  # was 100 → now closer to center
    frame_width = 1280

    roi_coords_left_lane = (side_offset, roi_y, roi_width, roi_height)
    roi_coords_right_lane = (frame_width - side_offset - roi_width, roi_y, roi_width, roi_height)

    PIXELS_PER_METER = calibrate_pixels_per_meter()

    vehicle_data = []
    previous_vehicles = {}
    current_track_id = 0
    frame_count = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        current_detections = []

        for roi_coords in [roi_coords_left_lane, roi_coords_right_lane]:
            x, y, w, h = roi_coords
            roi = frame[y:y+h, x:x+w]
            results = model(roi)[0]

            for box in results.boxes:
                x1, y1, x2, y2 = map(int, box.xyxy[0])
                cls_id = int(box.cls.item())
                if cls_id in [2, 3, 5, 7]:
                    current_detections.append({
                        "bbox": (x1 + x, y1 + y, x2 + x, y2 + y),
                        "class": cls_id
                    })

        updated_vehicles = {}
        matched_ids = set()

        for det in current_detections:
            best_match_id = None
            best_distance = float('inf')
            det_center = np.array([(det["bbox"][0] + det["bbox"][2]) / 2, 
                                   (det["bbox"][1] + det["bbox"][3]) / 2])

            for track_id, vehicle in previous_vehicles.items():
                if track_id in matched_ids:
                    continue
                prev_center = np.array([(vehicle["bbox"][0] + vehicle["bbox"][2]) / 2,
                                        (vehicle["bbox"][1] + vehicle["bbox"][3]) / 2])
                distance = np.linalg.norm(det_center - prev_center)

                if distance < MAX_DISTANCE and distance < best_distance:
                    best_distance = distance
                    best_match_id = track_id

            if best_match_id is not None:
                updated_vehicles[best_match_id] = {
                    "bbox": det["bbox"],
                    "first_frame": previous_vehicles[best_match_id]["first_frame"],
                    "bbox_history": previous_vehicles[best_match_id]["bbox_history"] + [det["bbox"]],
                    "class": det["class"]
                }
                matched_ids.add(best_match_id)
            else:
                current_track_id += 1
                updated_vehicles[current_track_id] = {
                    "bbox": det["bbox"],
                    "first_frame": frame_count,
                    "bbox_history": [det["bbox"]],
                    "class": det["class"]
                }

        # Finalize exited vehicles
        for track_id, vehicle in previous_vehicles.items():
            if track_id not in updated_vehicles and len(vehicle["bbox_history"]) >= 2:
                first_bbox = vehicle["bbox_history"][0]
                last_bbox = vehicle["bbox_history"][-1]

                first_center = np.array([(first_bbox[0] + first_bbox[2]) / 2, 
                                         (first_bbox[1] + first_bbox[3]) / 2])
                last_center = np.array([(last_bbox[0] + last_bbox[2]) / 2, 
                                        (last_bbox[1] + last_bbox[3]) / 2])

                displacement_pixels = np.linalg.norm(last_center - first_center)
                displacement_meters = displacement_pixels / PIXELS_PER_METER
                time_seconds = (frame_count - vehicle["first_frame"]) / FRAME_RATE
                speed_kmh = (displacement_meters / time_seconds) * 3.6

                if speed_kmh >= MIN_SPEED_KMH:
                    vehicle_types = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}
                    vehicle_type = vehicle_types.get(vehicle["class"], "unknown")
                    lane = 0 if first_center[0] < width // 2 else 1

                    vehicle_data.append({
                        "track_id": track_id,
                        "time_entered": vehicle["first_frame"] / FRAME_RATE,
                        "time_exited": frame_count / FRAME_RATE,
                        "speed_kmh": round(speed_kmh, 2),
                        "vehicle_type": vehicle_type,
                        "lane": lane,
                        "is_speeding": speed_kmh > SPEED_LIMIT
                    })

        previous_vehicles = updated_vehicles
        frame_count += 1

        # Draw ROIs
        draw_rectangle(frame, *roi_coords_left_lane, (0, 255, 0))
        draw_rectangle(frame, *roi_coords_right_lane, (255, 0, 0))

        # Draw bounding boxes and info
        for track_id, vehicle in previous_vehicles.items():
            x1, y1, x2, y2 = vehicle["bbox"]
            cls = vehicle["class"]
            vehicle_types = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}
            label = f"ID:{track_id} {vehicle_types.get(cls, 'unknown')}"
            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 2)
            cv2.putText(frame, label, (x1, y1 - 10), 
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 255), 2)

        cv2.imshow('Processed Frame', frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    cap.release()
    cv2.destroyAllWindows()

    # Export CSV
    df = pd.DataFrame(vehicle_data)
    df.to_csv(output_csv, index=False)
    logging.info(f"Saved CSV to {output_csv}")


def draw_rectangle(frame, x, y, w, h, color):
    cv2.rectangle(frame, (x, y), (x + w, y + h), color, 2)


# Run it
analyze_video("videos_clips_csvs/10s_clips/clip_0.mp4", "videos_clips_csvs/results.csv")