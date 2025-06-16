import cv2
import pandas as pd
from ultralytics import YOLO
import os
import re

def analyse_clip(video_path, csv_output_path, show_video=False):
    """
    Analyze a video clip for vehicle detection, speed calculation, and traffic monitoring.
    
    Args:
        video_path (str): Path to the input video file
        csv_output_path (str): Path where the CSV file will be saved
        show_video (bool): Whether to display the video during processing (default: False)
    
    Returns:
        pd.DataFrame: DataFrame containing vehicle data with columns:
                     ['Id', 'timeEntered', 'speed', 'vehicleType', 'lane', 'speeding']
    """
    
    # Validate input paths
    if not os.path.exists(video_path):
        raise FileNotFoundError(f"Video file not found: {video_path}")
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(csv_output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Get the last character of the file name in order to set correct relative time
    # Get the base name without extension
    base = os.path.splitext(video_path)[0] #videos_clips_csvs/10s_clips/clip_0

    # Use regex to find the number at the end of the base name
    match = re.search(r'(\d+)$', base)
    if match:
        clip_number = int(match.group(1))
        print("Clip number:", clip_number)
    else:
        print("No number found in file name.")

    # Load YOLOv8 model
    model = YOLO('yolov8n.pt')

    # Open video file
    cap = cv2.VideoCapture(video_path)
    
    if not cap.isOpened():
        raise ValueError(f"Could not open video file: {video_path}")

    # Define ROIs (x, y, width, height)
    roi_left = (150, 400, 400, 140)
    roi_right = (730, 400, 400, 140)

    # Exit lines
    line_y_left = roi_left[1] + roi_left[3] // 2
    line_y_right = roi_right[1] + roi_right[3] // 2

    # Real ROI length in meters
    roi_length_m = 10.0

    # Maximum allowed speed to dismiss vehicles (e.g., glitch)
    MAX_SPEED_THRESHOLD_CAR = 300.0
    MAX_SPEED_THRESHOLD_TRUCK = 200.0

    # Speed limits
    CAR_LIMIT = 90.0
    TRUCK_LIMIT = 80.0

    # Minimum time in frames to consider a valid crossing
    MIN_TIME_FRAMES = 5

    # Get FPS
    fps = cap.get(cv2.CAP_PROP_FPS)
    if fps == 0:
        fps = 30.0

    # Tracking dictionaries
    entry_frames_left = {}
    exit_frames_left = {}
    counted_left = set()

    entry_frames_right = {}
    exit_frames_right = {}
    counted_right = set()

    dismissed_vehicles = set()
    last_positions = {}

    # Output storage
    columns = ['vehicleId', 'timeEntered', 'speed', 'vehicleType', 'lane', 'speeding']
    vehicle_data = []

    def crossed_line(prev_y, curr_y, line_y):
        return (prev_y < line_y <= curr_y) or (prev_y > line_y >= curr_y)

    print(f"Processing video: {video_path}")
    print(f"FPS: {fps}")
    
    frame_count = 0
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        
        frame_count += 1
        if frame_count % 100 == 0:
            print(f"Processing frame {frame_count}/{total_frames}")

        results = model.track(frame, persist=True, conf=0.5, verbose=False)

        if results[0].boxes is not None and results[0].boxes.id is not None:
            for box, cls_id_tensor, track_id_tensor in zip(results[0].boxes.xyxy, results[0].boxes.cls, results[0].boxes.id):
                cls_id = int(cls_id_tensor)
                track_id = int(track_id_tensor)

                if cls_id not in [2, 5, 7]:
                    continue
                if cls_id == 5:
                    cls_id = 7  # Treat bus as truck

                if track_id in dismissed_vehicles:
                    continue

                x1, y1, x2, y2 = map(int, box)
                cx = (x1 + x2) // 2
                cy = (y1 + y2) // 2

                inside_left = roi_left[0] <= cx <= roi_left[0] + roi_left[2] and roi_left[1] <= cy <= roi_left[1] + roi_left[3]
                inside_right = roi_right[0] <= cx <= roi_right[0] + roi_right[2] and roi_right[1] <= cy <= roi_right[1] + roi_right[3]

                prev_pos = last_positions.get(track_id, (cx, cy))

                # LEFT
                if inside_left and track_id not in entry_frames_left:
                    entry_frames_left[track_id] = int(cap.get(cv2.CAP_PROP_POS_FRAMES))

                if track_id in entry_frames_left and track_id not in counted_left:
                    if crossed_line(prev_pos[1], cy, line_y_left):
                        exit_frame = int(cap.get(cv2.CAP_PROP_POS_FRAMES))
                        time_frames = exit_frame - entry_frames_left[track_id]
                        if time_frames > MIN_TIME_FRAMES:
                            speed = (roi_length_m / (time_frames / fps)) * 3.6
                            if speed > (MAX_SPEED_THRESHOLD_CAR if cls_id == 2 else MAX_SPEED_THRESHOLD_TRUCK):
                                dismissed_vehicles.add(track_id)
                                entry_frames_left.pop(track_id, None)
                                print(f"Vehicle ID {track_id} dismissed - unrealistic speed: {speed:.1f} km/h")
                                continue

                            vehicle_type = "car" if cls_id == 2 else "truck"
                            speeding = int((speed > CAR_LIMIT) if cls_id == 2 else (speed > TRUCK_LIMIT))
                            time_seconds = (entry_frames_left[track_id] / fps) + (clip_number-1)*120
                            vehicle_data.append([track_id, round(time_seconds, 2), round(speed, 1), vehicle_type, "out", speeding])
                            counted_left.add(track_id)
                        else:
                            print(f"Vehicle ID {track_id} dismissed - too short time: {time_frames} frames")
                            dismissed_vehicles.add(track_id)
                            entry_frames_left.pop(track_id, None)
                            continue

                # RIGHT
                if inside_right and track_id not in entry_frames_right:
                    entry_frames_right[track_id] = int(cap.get(cv2.CAP_PROP_POS_FRAMES))

                if track_id in entry_frames_right and track_id not in counted_right:
                    if crossed_line(prev_pos[1], cy, line_y_right):
                        exit_frame = int(cap.get(cv2.CAP_PROP_POS_FRAMES))
                        time_frames = exit_frame - entry_frames_right[track_id]
                        if time_frames > MIN_TIME_FRAMES:
                            speed = (roi_length_m / (time_frames / fps)) * 3.6
                            if speed > (MAX_SPEED_THRESHOLD_CAR if cls_id == 2 else MAX_SPEED_THRESHOLD_TRUCK):
                                dismissed_vehicles.add(track_id)
                                entry_frames_right.pop(track_id, None)
                                print(f"Vehicle ID {track_id} dismissed - unrealistic speed: {speed:.1f} km/h")
                                continue

                            vehicle_type = "car" if cls_id == 2 else "truck"
                            speeding = int((speed > CAR_LIMIT) if cls_id == 2 else (speed > TRUCK_LIMIT))
                            time_seconds = (entry_frames_right[track_id] / fps) + (clip_number-1)*120
                            vehicle_data.append([track_id, round(time_seconds, 2), round(speed, 1), vehicle_type, "in", speeding])
                            counted_right.add(track_id)
                        else:
                            print(f"Vehicle ID {track_id} dismissed - too short time: {time_frames} frames")
                            dismissed_vehicles.add(track_id)
                            entry_frames_right.pop(track_id, None)
                            continue

                # Draw bounding box (only if showing video)
                if show_video:
                    label = f"{model.names[cls_id]} ID:{track_id}"
                    color = (0, 255, 0) if cls_id == 2 else (255, 0, 0)

                    cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
                    cv2.putText(frame, label, (x1, y1 - 6), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)

                last_positions[track_id] = (cx, cy)

        # Draw ROIs and exit lines (only if showing video)
        if show_video:
            cv2.rectangle(frame, (roi_left[0], roi_left[1]), (roi_left[0] + roi_left[2], roi_left[1] + roi_left[3]), (0, 255, 255), 2)
            cv2.line(frame, (roi_left[0], line_y_left), (roi_left[0] + roi_left[2], line_y_left), (0, 0, 255), 2)

            cv2.rectangle(frame, (roi_right[0], roi_right[1]), (roi_right[0] + roi_right[2], roi_right[1] + roi_right[3]), (255, 0, 255), 2)
            cv2.line(frame, (roi_right[0], line_y_right), (roi_right[0] + roi_right[2], line_y_right), (0, 255, 255), 2)

            # Display total counts and dismissed count
            cv2.putText(frame, f"Left ROI Count: {len(counted_left)}", (20, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 255), 2)
            cv2.putText(frame, f"Right ROI Count: {len(counted_right)}", (20, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 0, 255), 2)
            cv2.putText(frame, f"Dismissed: {len(dismissed_vehicles)}", (20, 90), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 255), 2)

            # Display live video
            cv2.imshow("Vehicle Monitor", frame)
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

    cap.release()
    if show_video:
        cv2.destroyAllWindows()

    # Convert to DataFrame
    df = pd.DataFrame(vehicle_data, columns=columns)
    
    # Save to CSV
    df.to_csv(csv_output_path, index=False)
    
    print(f"\nAnalysis complete!")
    print(f"Total vehicles detected: {len(df)}")
    print(f"Left lane vehicles: {len(counted_left)}")
    print(f"Right lane vehicles: {len(counted_right)}")
    print(f"Dismissed vehicles: {len(dismissed_vehicles)}")
    print(f"Results saved to: {csv_output_path}")