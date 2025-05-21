from moviepy.editor import VideoFileClip
import os

video = VideoFileClip("videos_clips/video/vid_segment.mp4")
clip_duration = 120  # σε δευτερόλεπτα

output_dir = "videos_clips/clips"
os.makedirs(output_dir, exist_ok=True)

for i in range(0, int(video.duration), clip_duration):
    subclip = video.subclip(i, min(i + clip_duration, video.duration))
    subclip.write_videofile(f"{output_dir}/clip_{i // clip_duration}.mp4", codec="libx264")