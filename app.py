import requests
import sqlite3
import time
import os
import queue
import threading
import csv
import ffmpeg
import datetime
import yt_dlp
import pickle
from tiktok_uploader.upload import upload_video
import logging

# Configuration
Twitch_API_URL = "https://api.twitch.tv/helix/clips"
CLIENT_ID = "tpi212qa424cievjqoblfdoqm3kapl"
CLIENT_SECRET = "7n80kkkb5qx4azzipv88dgsylrz5o4"
BEARER_TOKEN = "p4l9pj0zqzdqkn9x2q8kk0evuuw7n5"
TIKTOK_USERNAME = "pogclips93"
TIKTOK_PASSWORD = "&&Garage123"
DATABASE = "clips.db"
CLIP_DOWNLOAD_DIR = "clips"  # Directory for downloaded Twitch clips
OUTPUT_DIR = "output"  # Directory for processed videos
STREAMER_CSV_FILE = 'streamers.csv'  # CSV file that holds streamer names and IDs
MAX_CLIPS = 1  # Maximum number of clips allowed in the directories
MAX_FETCHING = 1

# Configure logging to log to a file
logging.basicConfig(filename='upload_log.txt', level=logging.DEBUG)

# Load streamers from the CSV file
STREAMER_LIST = []
def load_streamers_from_csv():
    global STREAMER_LIST
    try:
        with open(STREAMER_CSV_FILE, mode='r') as file:
            reader = csv.reader(file)
            for row in reader:
                if row:  # Ensure there's data
                    streamer_name, streamer_id = row
                    STREAMER_LIST.append({'name': streamer_name, 'id': streamer_id})
        print(f"Loaded {len(STREAMER_LIST)} streamers from CSV.")
    except Exception as e:
        print(f"Error loading streamers from CSV: {e}")

# Create necessary directories if they don't exist
os.makedirs(CLIP_DOWNLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Queues for different stages
process_queue = queue.Queue()
download_queue = queue.Queue()
processing_queue = queue.Queue()
upload_queue = queue.Queue()
delete_queue = queue.Queue()  # New queue for deletion tasks

# Define locks for each queue
process_queue_lock = threading.Lock()
download_queue_lock = threading.Lock()
processing_queue_lock = threading.Lock()
upload_queue_lock = threading.Lock()
delete_queue_lock = threading.Lock()

def save_queues():
    # Save each queue to a separate file
    with open('process_queue.pkl', 'wb') as f:
        pickle.dump(list(process_queue.queue), f)
    with open('download_queue.pkl', 'wb') as f:
        pickle.dump(list(download_queue.queue), f)
    with open('processing_queue.pkl', 'wb') as f:
        pickle.dump(list(processing_queue.queue), f)
    with open('upload_queue.pkl', 'wb') as f:
        pickle.dump(list(upload_queue.queue), f)
    with open('delete_queue.pkl', 'wb') as f:
        pickle.dump(list(delete_queue.queue), f)

    #print("Queues saved.")

def load_queues():
    # Load each queue from its respective file
    try:
        with open('process_queue.pkl', 'rb') as f:
            process_queue.queue = pickle.load(f)
        with open('download_queue.pkl', 'rb') as f:
            download_queue.queue = pickle.load(f)
        with open('processing_queue.pkl', 'rb') as f:
            processing_queue.queue = pickle.load(f)
        with open('upload_queue.pkl', 'rb') as f:
            upload_queue.queue = pickle.load(f)
        with open('delete_queue.pkl', 'rb') as f:
            delete_queue.queue = pickle.load(f)

        print("Queues loaded successfully.")
    except FileNotFoundError:
        print("No saved queues found. Starting fresh.")

# Setup SQLite database to track processed clips
def init_db():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS clips (id TEXT PRIMARY KEY)''')
    conn.commit()
    conn.close()

def is_clip_processed(clip_id):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM clips WHERE id=?", (clip_id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def save_clip_to_db(clip_id):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO clips (id) VALUES (?)", (clip_id,))
    conn.commit()
    conn.close()

def remove_clip_from_db(clip_id):
    try:
        conn = sqlite3.connect(DATABASE)  # Connect to the database
        cursor = conn.cursor()
        
        # Execute the SQL query to delete the clip with the given ID
        cursor.execute("DELETE FROM clips WHERE id = ?", (clip_id,))
        
        # Commit the transaction
        conn.commit()
        
        # Check if the clip was deleted
        if cursor.rowcount > 0:
            print(f"Clip {clip_id} successfully removed from the database.")
        else:
            print(f"Clip {clip_id} not found in the database.")
        
        # Close the connection
        conn.close()
    except Exception as e:
        print(f"Error removing clip {clip_id} from the database: {str(e)}")

def check_max_clips(dir):
    count = len(os.listdir(dir))
    
    if count >= MAX_CLIPS:
        return False
    return True

# Function to fetch clips from Twitch API for a single streamer
def fetch_clips(streamer_id, streamer_name):
    headers = {
        'Client-ID': CLIENT_ID,
        'Authorization': f'Bearer {BEARER_TOKEN}',
    }
    
    # Calculate the timestamp for 24 hours ago in RFC3339 format
    twenty_four_hours_ago = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    started_at = twenty_four_hours_ago.isoformat() + 'Z'  # Convert to RFC3339 (UTC)

    params = {
        'broadcaster_id': streamer_id,
        'first': MAX_FETCHING,  # Get the latest 5 clips for each streamer
        'started_at': started_at  # Clips from the last 24 hours
    }

    # Make the request to Twitch API
    response = requests.get(Twitch_API_URL, headers=headers, params=params)
    
    if response.status_code == 200:
        clips = response.json().get('data', [])
        #print(f"Fetched {len(clips)} clips for streamer {streamer_name}.")
        return clips
    else:
        print(f"Error fetching clips for {streamer_name} (ID: {streamer_id}): {response.status_code}")
        return []

# Worker to fetch clips for streamers in batches, respecting rate limit
def fetch_clips_for_streamers():
    doneOnce = False
    while not shutdown_flag and not doneOnce:
        for streamer in STREAMER_LIST:
            streamer_name = streamer['name']
            streamer_id = streamer['id']
            print(f"Fetching clips for streamer: {streamer_name} (ID: {streamer_id})")
            clips = fetch_clips(streamer_id, streamer_name)
            for clip in clips:
                if shutdown_flag:
                    break
                clip_id = clip['id']
                clip_url = clip['url']
                clip_title = clip['title']
                if not is_clip_processed(clip_id):
                    print(f"Found new clip for {streamer_name}")
                    process_queue.put((clip_id, clip_url, streamer_name, clip_title))
                    doneOnce = True
                    break
            time.sleep(5)  # Wait 40 seconds before fetching the next batch of streamers

# Worker to process the queue and check if a clip is already processed
def process_queue_worker():
    while not shutdown_flag:
        try:
            with process_queue_lock:  # Ensure only one thread accesses the queue at a time
                clip_id, clip_url, streamer_name, clip_title = process_queue.get(timeout=5)
                #print(f"Processing clip {clip_id} from {streamer_name}...")

            if not is_clip_processed(clip_id):
                save_clip_to_db(clip_id)

                # Lock the download queue before putting the task in it
                with download_queue_lock:
                    download_queue.put((clip_id, clip_url, streamer_name, clip_title))
            
            process_queue.task_done()
            save_queues()  # Save queues to file after task is done
        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error in process queue worker: {e}")
            time.sleep(1)

# Worker to download the clip
def download_worker():
    while not shutdown_flag:
        if not check_max_clips(CLIP_DOWNLOAD_DIR):  # Check if the maximum number of clips is reached
            #print("Max clips reached, waiting for space...")
            time.sleep(1)  # Wait before checking again
            continue
        
        try:
            with download_queue_lock:
                clip_id, clip_url, streamer_name, clip_title = download_queue.get(timeout=5)
                print(f"Downloading clip {clip_id} from {streamer_name}...")
                
                # Download the clip and make sure it's downloaded completely
                clip_filename = download_clip(clip_url, clip_id)

                if clip_filename:
                    print(f"Successfully downloaded clip {clip_id} from {streamer_name}.")
                    # Once downloaded, add to the processing queue
                    processing_queue.put((clip_id, clip_filename, streamer_name, clip_title))
                else:
                    print(f"Failed to download clip {clip_id} from {streamer_name}.")

                # Mark this task as complete (only after everything is done)
                download_queue.task_done()
                save_queues()
        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error in process queue worker: {e}")
            time.sleep(1)

def resize_and_center_clip(clip_filename, output_filename, title, vertical_height=1920, vertical_width=1080):
    try:
        # Probe the clip for width and height
        probe = ffmpeg.probe(clip_filename, v='error', select_streams='v:0', show_entries='stream=width,height')
        clip_width = probe['streams'][0]['width']
        clip_height = probe['streams'][0]['height']
        print(f"Original clip dimensions: {clip_width}x{clip_height}")

        # Calculate the aspect ratio and adjust the dimensions
        aspect_ratio = clip_width / clip_height
        new_height = vertical_height
        new_width = int(new_height * aspect_ratio)

        if new_width > vertical_width:
            new_width = vertical_width
            new_height = int(new_width / aspect_ratio)

        print(f"New clip dimensions: {new_width}x{new_height}")

        # Calculate the y-position to place the text in the top third
        text_y_position = vertical_height // 3 / 2  # Adjust based on font size (30px above the center of the top third)

        # Absolute path to the font file
        font_path = "Arial"
        
        # Resize and center the clip, and overlay the title text in the top third
        ffmpeg.input(clip_filename).output(output_filename, 
                                           vf=f'scale={new_width}:{new_height},'
                                              f'pad={vertical_width}:{vertical_height}:(ow-iw)/2:(oh-ih)/2,'
                                              f'drawtext=text={title}:x=(w-text_w)/2:y={text_y_position}:fontsize=50:fontcolor=white:fontfile={font_path}',
                                           vcodec='libx264', acodec='aac', loglevel='error').run()

        return output_filename
        #print(f"Processed clip saved as {output_filename}")
    except ffmpeg.Error as e:
        print(f"ffmpeg error: {e.stderr.decode()}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Worker to apply resize and center to the clip
def processing_worker():
    while not shutdown_flag:
        # Check if the output directory has reached the maximum number of clips
        if not check_max_clips(OUTPUT_DIR):  # Use the check_max_clips function to check the output folder
            #print("Max clips reached in output directory, waiting for space...")
            time.sleep(1)  # Wait before checking again
            continue  # Skip processing this round and check again

        try:
            with processing_queue_lock:
                clip_id, clip_filename, streamer_name, title = processing_queue.get(timeout=5)
                print(f"Processing clip {clip_id} from {streamer_name}...")

            # Output filename
            output_video_filename = os.path.join(OUTPUT_DIR, f"output_{clip_id}.mp4")

            # Resize, center, and add title text
            resize_and_center_clip(clip_filename, output_video_filename, title)

            # Add the processed clip to the upload queue
            upload_queue.put((clip_id, output_video_filename, streamer_name))
            
            processing_queue.task_done()
            save_queues()  # Save the queue after processing a clip

        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error in process queue worker: {e}")
            time.sleep(1)

# Function to upload video to TikTok using the tiktok-uploader library
def upload_to_tiktok(video_path, streamer):
    try:
        logging.info(f"Uploading {video_path} to TikTok...")
        caption = f"#fyp #{streamer} #viral #viralvideo #foryoupage #twitch #twitchstreamer #clip #funny"
        cookie_file = 'cookies.txt'  # Path to your TikTok cookies file

        # Upload the video
        return upload_video(video_path, caption, cookie_file)
    except Exception as e:
        logging.error(f"Failed to upload {video_path} to TikTok. Error: {e}")
        return False

# Worker to upload the video to TikTok using tiktok-uploader
def upload_worker():
    while not shutdown_flag:
        try:
            with upload_queue_lock:
                clip_id, final_video, streamer_name = upload_queue.get(timeout=5)

            print(f"Uploading clip {clip_id} from {streamer_name}...")

            # Retry the upload until it succeeds
            retries = 0
            while retries < 5:
                if upload_to_tiktok(final_video, streamer_name):
                    print(f"Successfully uploaded {clip_id} to TikTok.")
                    break
                else:
                    retries += 1
                    print(f"Failed to upload clip {clip_id} to TikTok. Retrying in 5 seconds... (Attempt {retries}/5)")
                    time.sleep(5)

            if retries == 5:
                print(f"Failed to upload clip {clip_id} after 5 attempts. Moving to next task.")
                continue

            # After successful upload, remove clip from DB and add to delete queue
            remove_clip_from_db(clip_id)
            delete_queue.put((clip_id, final_video))

            upload_queue.task_done()
            save_queues()

        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error in process queue worker: {e}")
            time.sleep(1)

# Worker to delete files once uploaded
def delete_worker():
    while not shutdown_flag:
        try:
            with delete_queue_lock:
                clip_id, final_video = delete_queue.get(timeout=5)

            print(f"Deleting clip {clip_id} and its output video...")

            # Path to the downloaded clip and processed video
            clip_filename = os.path.join(CLIP_DOWNLOAD_DIR, f"{clip_id}.mp4")
            retry_attempts = 3
            deleted = False

            # Retry deletion logic in case files are being used or any other issues occur
            while retry_attempts > 0 and not deleted:
                try:
                    # Delete the original downloaded clip if it exists
                    if os.path.exists(clip_filename):
                        os.remove(clip_filename)
                        print(f"Deleted downloaded clip: {clip_filename}")
                    
                    # Delete the processed video file if it exists
                    if os.path.exists(final_video):
                        os.remove(final_video)
                        print(f"Deleted processed video file: {final_video}")
                    
                    deleted = True  # Successfully deleted the files

                except Exception as e:
                    retry_attempts -= 1
                    print(f"Error deleting files for clip {clip_id}: {str(e)}. Retries left: {retry_attempts}")
                    if retry_attempts == 0:
                        print(f"Failed to delete files for clip {clip_id} after multiple attempts.")
                        break
                    time.sleep(2)  # Sleep before retrying deletion

            # Mark the task as done after attempting to delete
            delete_queue.task_done()

            # Save the state of the queues after deletion attempt
            save_queues()

        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error in process queue worker: {e}")
            time.sleep(1)

# Function to download a clip using yt-dlp
def download_clip(clip_url, clip_id):
    clip_filename = os.path.join(CLIP_DOWNLOAD_DIR, f"{clip_id}.mp4")

    # yt-dlp options to download the best quality and specify the output filename
    ydl_opts = {
        'outtmpl': clip_filename,  # Output the clip with clip_id.mp4 filename
        'format': 'best',  # Download the best quality available
        'quiet': False,  # Set to True if you want to silence yt-dlp's output
    }

    try:
        # Use yt-dlp to download the clip
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([clip_url])
        
        print(f"Downloaded clip {clip_id} successfully!")
        return clip_filename
    except Exception as e:
        print(f"Failed to download clip {clip_id}. Error: {str(e)}")
        return None
    
# Main monitoring function
def main():
    global shutdown_flag
    shutdown_flag = False

    load_streamers_from_csv()  # Load streamers from CSV file
    init_db()
    load_queues()  # Load queues from file if available

    # Start worker threads
    threading.Thread(target=fetch_clips_for_streamers, daemon=True).start()
    threading.Thread(target=process_queue_worker, daemon=True).start()
    threading.Thread(target=download_worker, daemon=True).start()
    threading.Thread(target=processing_worker, daemon=True).start()
    threading.Thread(target=upload_worker, daemon=True).start()
    threading.Thread(target=delete_worker, daemon=True).start()

    try:
        while True:
            time.sleep(10)  # Keep the main thread alive
    except KeyboardInterrupt:
        # Graceful shutdown triggered by keyboard interrupt
        print("Graceful shutdown initiated...")
        shutdown_flag = True  # Set the shutdown flag to stop workers
        
        # Wait for all worker threads to finish processing tasks
        print("Waiting for all tasks to complete...")
        process_queue.join()  # Ensure all tasks in process_queue are done
        download_queue.join()  # Ensure all tasks in download_queue are done
        processing_queue.join()  # Ensure all tasks in processing_queue are done
        upload_queue.join()  # Ensure all tasks in upload_queue are done
        delete_queue.join()  # Ensure all tasks in delete_queue are done

        # Save the queues state to file before exit
        save_queues()

        print("Shutdown complete.")

if __name__ == "__main__":
    main()

# every 5 seconds fetch streamers last 10 clips in the last 24 hours
# for every clip check if it has been processed in database or in processing set
# if not been processed in both, then add clip id to processing set
# send clip to processing queue
# processing queue gets new clip and downloads, then resizes, then uploads clip, then deletes clips on successful upload
# after uploading and delteing, clip id can be removed from processing queue and processing set
# clip id can be saved in database