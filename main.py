import csv
import time
import datetime
import threading
import queue
import sqlite3
import requests
import ffmpeg
import yt_dlp
import os
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Configuration
TWITCH_API_URL = "https://api.twitch.tv/helix/clips"
TWITCH_CLIENT_ID = "tpi212qa424cievjqoblfdoqm3kapl"
TWITCH_CLIENT_SECRET = "7n80kkkb5qx4azzipv88dgsylrz5o4"
TWITCH_BEARER_TOKEN = "p4l9pj0zqzdqkn9x2q8kk0evuuw7n5"
TIKTOK_USERNAME = "pogclips93"
TIKTOK_PASSWORD = "&&Garage123"
DATABASE = "clips.db"
CLIP_DOWNLOAD_DIR = "clips"
OUTPUT_DIR = "output"
STREAMER_CSV_FILE = 'streamers.csv'
MAX_CLIPS = 20
MAX_FETCHING = 1
MAXIMUM_FETCH = 20
FETCH_INTERVAL = 10

# In-memory tracking
processing_set = set()
processing_queue = queue.Queue()
browser = None

RESET = "\033[0m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"
WHITE = "\033[37m"
states = {
    "fetching": "",
    "streamer": "",
    "clip": "",
    "status": "",
    "last": "",
    "directory": False
}
status = {
    "DOWNLOADING": f"{YELLOW}DOWNLOADING{RESET}",
    "PROCESSING": f"{BLUE}PROCESSING{RESET}",
    "UPLOADING": f"{MAGENTA}UPLOADING{RESET}",
    "SAVING": f"{CYAN}SAVING{RESET}",
    "COMPLETE": f"{GREEN}COMPLETE{RESET}",
}

def print_state():
    os.system("cls")

    print(f"Fetching Streamer: {states['fetching']}")
    print(f"[{states['status']}] {states['streamer']} | {states['clip']}")
    print("")
    print(f"Last Completed: {states['last']}")
    print(f"Processed Queue Length: {len(processing_set)}")
    print(f"Processing Queue Length: {processing_queue.qsize()}")
    print(f"Max Directory Files Reached: {states['directory']}")

def update_state(state, update):
    states[state] = update

# Initialize the lock
processing_lock = threading.Lock()

# Thread-safe method to add a clip_id to the processing set
def add_to_processing_set(clip_id):
    with processing_lock:
        processing_set.add(clip_id)

# Thread-safe method to remove a clip_id from the processing set
def remove_from_processing_set(clip_id):
    with processing_lock:
        processing_set.remove(clip_id)

# Database connection
def init_db():
    try:
        # Use a context manager to ensure the connection and cursor are closed properly
        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS clips (id TEXT PRIMARY KEY)''')
            # No need to call conn.commit() here, it's done automatically when using `with` context
    except sqlite3.Error as e:
        print(f"Error initializing database: {e}")
        exit(1)

def save_clip_to_db(clip_id):
    try:
        # Use a context manager to ensure the connection and cursor are closed properly
        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO clips (id) VALUES (?)", (clip_id,))
            conn.commit()
    except sqlite3.Error as e:
        print(f"Error saving clip {clip_id} to database: {e}")
        exit()

def init_browser():
    """Start Selenium and log in to TikTok."""
    global browser
    options = webdriver.ChromeOptions()
    options.add_experimental_option("detach", True)  # Keep browser open
    browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # Go to TikTok login page
    browser.get("https://www.tiktok.com/login")

    print("Log in to TikTok manually, then press Enter here.")
    print(TIKTOK_USERNAME)
    print(TIKTOK_PASSWORD)
    input("Press Enter after logging in...")  # Wait for user to confirm login

def load_streamers(csv_file):
    streamers = []
    with open(csv_file, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) == 2:
                streamers.append((row[0], row[1]))  # (name, id)
    return streamers

def is_clip_processed(clip_id):
    # Use a context manager to ensure the connection is closed properly
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM clips WHERE id=?", (clip_id,))
        result = cursor.fetchone()

    # Return True if the clip is found (exists in the database), else False
    return result is not None

def check_max_clips(dir):
    count = len(os.listdir(dir))
    
    if count >= MAX_CLIPS:
        return True
    return False

# Function to fetch clips from Twitch API for a single streamer
def fetch_clips(streamer_id, streamer_name):
    headers = {
        'Client-ID': TWITCH_CLIENT_ID,
        'Authorization': f'Bearer {TWITCH_BEARER_TOKEN}',
    }
    
    # Calculate the timestamp for 24 hours ago in RFC3339 format
    twenty_four_hours_ago = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    started_at = twenty_four_hours_ago.isoformat() + 'Z'  # Convert to RFC3339 (UTC)

    params = {
        'broadcaster_id': streamer_id,
        'first': fetching,  # Get the latest 5 clips for each streamer
        'started_at': started_at  # Clips from the last 24 hours
    }

    # Make the request to Twitch API
    response = requests.get(TWITCH_API_URL, headers=headers, params=params)
    
    if response.status_code == 200:
        clips = response.json().get('data', [])
        #print(f"Fetched {len(clips)} clips for streamer {streamer_name}.")
        return clips
    else:
        print(f"Error fetching clips for {streamer_name} (ID: {streamer_id}): {response.status_code}")
        return []

def clean_text(text):
    # Remove emojis
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # Emoticons
        "\U0001F300-\U0001F5FF"  # Symbols & pictographs
        "\U0001F680-\U0001F6FF"  # Transport & map symbols
        "\U0001F700-\U0001F77F"  # Alchemical symbols
        "\U0001F780-\U0001F7FF"  # Geometric shapes
        "\U0001F800-\U0001F8FF"  # Supplemental arrows
        "\U0001F900-\U0001F9FF"  # Supplemental symbols & pictographs
        "\U0001FA00-\U0001FA6F"  # Chess symbols, etc.
        "\U0001FA70-\U0001FAFF"  # More pictographs
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE
    )
    text = emoji_pattern.sub(r'', text)  # Remove emojis

    # Remove special characters, keeping only letters, numbers, spaces
    text = re.sub(r'[^A-Za-z0-9\s]', '', text)

    return text
    
def process_fetched_clip(clip_id, clip_url, streamer_name, clip_title):
    with processing_lock:
        if not is_clip_processed(clip_id) and clip_id not in processing_set:
            #print(f"Adding New Clip to Queue: {streamer_name} | {clip_title}")
            cleaned_text  = clean_text(clip_title)
            processing_set.add(clip_id)
            processing_queue.put((clip_id, clip_url, streamer_name, cleaned_text))

def download_clip(clip_id, clip_url, streamer_name, clip_title):
    #print(f"Downloading Clip: {streamer_name} | {clip_title}")
            
    # Download the clip and make sure it's downloaded completely
    clip_filename = os.path.join(CLIP_DOWNLOAD_DIR, f"{clip_id}.mp4")

    # yt-dlp options to download the best quality and specify the output filename
    ydl_opts = {
        'outtmpl': clip_filename,  # Output the clip with clip_id.mp4 filename
        'format': 'best',  # Download the best quality available
        'quiet': True,  # Set to True if you want to silence yt-dlp's output
        'noprogress': True,
        'force_overwrites': True,
    }

    try:
        # Use yt-dlp to download the clip
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([clip_url])
        return clip_filename
    except Exception as e:
        print(f"Failed to download clip {clip_id}. Error: {str(e)}")
        return None

def resize_clip(clip_id, streamer_name, clip_filename, clip_title, vertical_height=1920, vertical_width=1080):
    try:
        #print(f"Processing Clip: {streamer_name} | {clip_title}")

        # Output filename
        output_filename = os.path.join(OUTPUT_DIR, f"output_{clip_id}.mp4")

        # Probe the clip for width and height
        probe = ffmpeg.probe(clip_filename, v='error', select_streams='v:0', show_entries='stream=width,height')
        clip_width = probe['streams'][0]['width']
        clip_height = probe['streams'][0]['height']
        #print(f"Original clip dimensions: {clip_width}x{clip_height}")

        # Calculate the aspect ratio and adjust the dimensions
        aspect_ratio = clip_width / clip_height
        new_height = vertical_height
        new_width = int(new_height * aspect_ratio)

        if new_width > vertical_width:
            new_width = vertical_width
            new_height = int(new_width / aspect_ratio)

        #print(f"New clip dimensions: {new_width}x{new_height}")

        # Calculate the y-position to place the text in the top third
        text_y_position = vertical_height // 3 / 2  # Adjust based on font size (30px above the center of the top third)

        # Resize and center the clip, and overlay the title text in the top third
        ffmpeg.input(clip_filename).output(output_filename, 
                                           vf=f'scale={new_width}:{new_height},'
                                              f'pad={vertical_width}:{vertical_height}:(ow-iw)/2:(oh-ih)/2,'
                                              f'drawtext=text={clip_title}:x=(w-text_w)/2:y={text_y_position}:fontsize=50:fontcolor=white:font=arial',
                                           vcodec='libx264', 
                                           acodec='aac', 
                                           loglevel='quiet').run()

        #print(f"Finished Processing: {streamer_name} | {clip_title}")
        return output_filename
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        #exit(1)

def delete_downloaded_clip(clip_id):
    clip_filename = os.path.join(CLIP_DOWNLOAD_DIR, f"{clip_id}.mp4")
    if os.path.exists(clip_filename):
        os.remove(clip_filename)
        #print(f"Deleted Downloaded Clip: {clip_filename}")

def upload_clip(clip_id, streamer_name, clip_title, final_clip_filename):
    if browser is None:
        print("Error: Browser session is not initialized.")
        return
    
    # Convert filename to absolute path
    full_file_path = os.path.abspath(final_clip_filename)

    # Go to TikTok upload page
    browser.get("https://www.tiktok.com/upload?lang=en")
    time.sleep(7)  # Wait for the page to load

    # Send the file path directly
    file_input = browser.find_element(By.XPATH, '//input[@type="file"]')
    file_input.send_keys(full_file_path)
    time.sleep(7)  # Wait for upload processing

    # Add caption
    caption_box = browser.find_element(By.XPATH, '//div[contains(@class, "notranslate public-DraftEditor-content")]')
    caption_box.click()
    caption_box.send_keys(Keys.CONTROL + "a")  # Select all text
    caption_box.send_keys(Keys.BACKSPACE)  # Delete selected text
    caption_box.send_keys(f"{streamer_name} | {clip_title}")
    caption_box.send_keys(Keys.ENTER)
    caption_box.send_keys(Keys.ENTER)
    caption_box.send_keys(f"#fyp #{streamer_name} #viral #viralvideo #foryoupage #twitch #twitchstreamer #clip #funny #lsf #livestreamfails")
    time.sleep(7)

    # Click the "Post" button
    post_button = browser.find_element(By.XPATH, '//button[@data-e2e="post_video_button"]')
    post_button.click()
    time.sleep(7)  # Wait for upload completion

    return browser.current_url == "https://www.tiktok.com/tiktokstudio/content"
        

def delete_output_clip(clip_id):
    clip_filename = os.path.join(OUTPUT_DIR, f"output_{clip_id}.mp4")
    if os.path.exists(clip_filename):
        os.remove(clip_filename)
        #print(f"Deleted Output Clip: {clip_filename}")

def processing_worker():
    while True:
        try:
            # Get next clip in queue
            item = processing_queue.get(timeout=3)
            if item is None:
                break

            clip_id, clip_url, streamer_name, clip_title = item

            # Acquire the lock before accessing the processing_set
            with processing_lock:
                if is_clip_processed(clip_id):
                    processing_set.remove(clip_id)
                    processing_queue.task_done()
                    #print("Skipping Clip Has Been Processed in Database")
                    continue

            update_state("streamer", streamer_name)
            update_state("clip", clip_title)
            update_state("status", status["DOWNLOADING"])

            # Check for the max clips condition and process the clip
            while not check_max_clips(OUTPUT_DIR):
                update_state("directory", False)

                # Download clip
                clip_filename = download_clip(clip_id, clip_url, streamer_name, clip_title)
                time.sleep(1)

                # Resize downloaded clip
                update_state("status", status["PROCESSING"])
                final_clip_filename = resize_clip(clip_id, streamer_name, clip_filename, clip_title)
                time.sleep(1)
                
                # Upload final clip to TikTok
                update_state("status", status["UPLOADING"])
                uploaded = upload_clip(clip_id, streamer_name, clip_title, final_clip_filename)
                time.sleep(1)

                # Delete downloaded clip
                delete_downloaded_clip(clip_id)
                delete_output_clip(clip_id)
                time.sleep(1)
                
                # Remove clip from processing set safely with the lock
                with processing_lock:
                    processing_set.remove(clip_id)

                # Save clip id to database
                if uploaded:
                    update_state("status", status["SAVING"])
                    save_clip_to_db(clip_id)

                # Mark task as complete
                update_state("status", status["COMPLETE"])
                update_state("last", f"{streamer_name} | {clip_title}")
                processing_queue.task_done()
                break  # Break out of the while loop to prevent endless checking

            else:
                print("Output Directory Full")
                update_state("directory", True)
                if stopped:
                    break
                time.sleep(1)

        except queue.Empty:
            print("Queue is empty, no clips to process.")
            if stopped:
                    break
            time.sleep(1)

def print_state_periodically():
    while not stopped:
        print_state()  # This will print the state every second
        time.sleep(1)  # Delay for 1 second before printing again


def main():
    global stopped
    stopped = False

    global fetching
    fetching = 1

    init_db()
    init_browser()

    streamers = load_streamers(STREAMER_CSV_FILE)
    worker_thread = threading.Thread(target=processing_worker, args=(), daemon=True)
    worker_thread.start()

    # Start a thread to print the state every second
    print_state_thread = threading.Thread(target=print_state_periodically, args=(), daemon=True)
    print_state_thread.start()

    try:
        while not stopped:
            for streamer_name, streamer_id in streamers:
                #print(f"Fetching Clips For {streamer_name}")
                update_state("fetching", streamer_name)
                clips = fetch_clips(streamer_id, streamer_name)
                
                for clip in clips:
                    clip_id = clip['id']
                    clip_url = clip['url']
                    clip_title = clip['title']
                    process_fetched_clip(clip_id, clip_url, streamer_name, clip_title)
                time.sleep(FETCH_INTERVAL)
            if fetching >= MAX_FETCHING:
                fetching = MAXIMUM_FETCH
            else:
                fetching = fetching + 1
            time.sleep(30)
    except KeyboardInterrupt:
        stopped = True
        print("Shutting down gracefully...")
        
        # Clear the queue, but keep the first item (if any) being processed
        with processing_queue.mutex:
            processing_queue.queue.clear()

        # Wait for the currently processing item to finish
        worker_thread.join()
        print("Shutdown complete.")

        if browser:
            browser.quit()

        exit(1)

if __name__ == "__main__":
    main()