import time
import logging
import cv2

# This function will use open cv to analyze the 2-min video segement
# Video is 1280px x 720px
def analyze_video(video_path):
    logging.info(" analysis started")
    cap = cv2.VideoCapture(video_path)

    if not cap.isOpened():
        print("Error: Could not open video.")
        exit()

    # Video info
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    logging.info(f"Video width: {width}px, height: {height}px")

    # Regions of interest (ROI) - (x, y, width, height)
    roi_coords_left_lane = (150, 350, 500, 250)
    roi_coords_right_lane = (700, 350, 450, 250)

    # Main loop for the video processing
    while True:
        ret, frame = cap.read()
        if not ret:
            print("End of video or failed to read frame.")
            break

        # Extract ROI
        x_l, y_l, w_l, h_l = roi_coords_left_lane
        roi_left  = frame[y_l:y_l+h_l, x_l:x_l+w_l]

        x_r, y_r, w_r, h_r = roi_coords_right_lane
        roi_right = frame[y_r:y_r+h_r, x_r:x_r+w_r]

        # Detect bodies

        # Condition

        # Draw ROI rectangles on the full frame and display them
        # Display will be removed when deploying to azure
        draw_rectangle(frame,x_l, w_l, y_l , h_l,(0, 255, 0))
        draw_rectangle(frame,x_r, w_r, y_r , h_r,(255, 0, 0))
        display(frame,[roi_left,roi_right])

        # --- Exit with 'q' key ---
        if cv2.waitKey(20) & 0xFF == ord('q'):
            break

    cap.release()
    logging.info(" analysis ended ")

#------Helper draw functions-------
def draw_rectangle(frame,x,w,y,h,color):
    cv2.rectangle(frame,(x, y),(x + w, y + h),color, 2)

def display(frame, roi_list, name_list=None):
    """
    Display the full frame and a list of ROIs with optional window names.

    Args:
        frame (np.ndarray): The full video frame.
        roi_list (list of np.ndarray): List of regions of interest(ROIs).
        name_list (list of str, optional): Names for each ROI window.
    """
    cv2.imshow('Full Frame', frame)

    for i, roi in enumerate(roi_list):
        name = name_list[i] if name_list and i < len(name_list) else f'ROI {i}'
        cv2.imshow(name, roi)

    
    