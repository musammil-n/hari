import asyncio
import feedparser
import logging
import threading
import requests
import io
import re
import time
import subprocess
from datetime import datetime
from flask import Flask
from pyrogram import Client, errors
from pyrogram.types import Message
from config import BOT, API, OWNER, CHANNEL
import aiohttp
import libtorrent as lt
import os
from PIL import Image
import tempfile

# ------------------ Keep-Alive URL ------------------
KEEP_ALIVE_URL = "https://willowy-donny-kushpu-0e1a566f.koyeb.app/"

# ** Peer ID Fix **
from pyrogram import utils as pyroutils
pyroutils.MIN_CHAT_ID = -999999999999
pyroutils.MIN_CHANNEL_ID = -10099999999999

# ------------------ Logging Setup ------------------
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)

# ------------------ Flask App for Health Check ------------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running!"

def run_flask():
    app.run(host='0.0.0.0', port=8000)

# ------------------ PSA RSS Feed ------------------
PSA_FEEDS = [
    "https://bt4gprx.com/search?q=psa&page=rss"
]

# ------------------ Configuration for Filters ------------------
# Maximum allowed file size in MB (1900 MB)
MAX_FILE_SIZE_MB = 1900

# Keywords to filter out (case-insensitive)
FILTER_KEYWORDS = [
    "predvd", "prehd", "hdts", "camrip", "telesync", "screener", "ts", "dvdscr"
]

# Directory to store downloaded files
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Thumbnail directory
THUMBNAIL_DIR = "thumbnails"
os.makedirs(THUMBNAIL_DIR, exist_ok=True)

# Telegram filename limit
TELEGRAM_FILENAME_LIMIT = 64

# ------------------ Libtorrent specific configurations ------------------
MAGNET_RESPONSE_TIMEOUT_SECONDS = 600 # 10 minutes

# ------------------ Utility Functions ------------------
def fetch_feed(url):
    return feedparser.parse(url)

def download_torrent_or_link(entry):
    if entry.enclosures:
        return entry.enclosures[0].href, 'torrent'
    link = entry.link
    if link.lower().endswith('.torrent'):
        return link, 'torrent'
    return link, 'link'

def format_message(entry):
    link = entry.link
    filename = entry.title
    desc = entry.get("description", "")
    size_match = re.search(r"(\d+(?:\.\d+)?\s*(?:GB|MB|KB))", desc, re.IGNORECASE)
    filesize = size_match.group(1) if size_match else "Unknown size"
    date_str = entry.get("published") or entry.get("updated") or "Unknown date"
    try:
        t = entry.published_parsed or entry.updated_parsed
        date_str = datetime.fromtimestamp(time.mktime(t)).strftime("%Y-%m-%d %H:%M")
    except:
        pass
    return f"**Title:** `{filename}`\n**Size:** `{filesize}`\n**Date:** `{date_str}`\n\n**Link:** `{link}`"

def is_valid_entry(entry):
    desc = entry.get("description", "")
    size_match = re.search(r"(\d+(?:\.\d+)?)\s*(GB|MB|KB)", desc, re.IGNORECASE)
    
    if size_match:
        size_value = float(size_match.group(1))
        size_unit = size_match.group(2).upper()

        size_mb = 0
        if size_unit == "GB":
            size_mb = size_value * 1024
        elif size_unit == "MB":
            size_mb = size_value
        elif size_unit == "KB":
            size_mb = size_value / 1024

        if size_mb > MAX_FILE_SIZE_MB:
            logging.info(f"Skipping '{entry.title}' due to large size: {size_value}{size_unit} (>{MAX_FILE_SIZE_MB}MB)")
            return False
    else:
        logging.debug(f"Could not determine size for '{entry.title}' from description. Will check magnet info later.")

    title = entry.title.lower()
    for keyword in FILTER_KEYWORDS:
        if keyword in title:
            logging.info(f"Skipping '{entry.title}' due to forbidden keyword: '{keyword}'")
            return False
            
    return True

def generate_thumbnail(video_path, output_path, timestamp="00:00:10"):
    """Generate thumbnail from video file using ffmpeg"""
    try:
        cmd = [
            'ffmpeg', '-i', video_path, '-ss', timestamp,
            '-vframes', '1', '-an', '-vcodec', 'png',
            '-f', 'rawvideo', '-s', '320x240', output_path, '-y'
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and os.path.exists(output_path):
            # Resize and optimize thumbnail
            with Image.open(output_path) as img:
                img.thumbnail((320, 240), Image.Resampling.LANCZOS)
                img.save(output_path, "JPEG", quality=85, optimize=True)
            return output_path
        else:
            logging.warning(f"ffmpeg failed to generate thumbnail: {result.stderr}")
            return None
    except subprocess.TimeoutExpired:
        logging.warning(f"ffmpeg timeout while generating thumbnail for {video_path}")
        return None
    except Exception as e:
        logging.error(f"Error generating thumbnail: {e}")
        return None

def create_default_thumbnail():
    """Create a default thumbnail image"""
    try:
        img = Image.new('RGB', (320, 240), color='black')
        # You could add text or logo here
        temp_path = os.path.join(THUMBNAIL_DIR, "default_thumb.jpg")
        img.save(temp_path, "JPEG", quality=85)
        return temp_path
    except Exception as e:
        logging.error(f"Error creating default thumbnail: {e}")
        return None

def add_suffix_to_filename(file_path, suffix=" -@MNTGX.-"):
    """Add suffix to filename while respecting Telegram's filename limit"""
    try:
        directory = os.path.dirname(file_path)
        filename = os.path.basename(file_path)
        base_name, ext = os.path.splitext(filename)
        
        full_suffix_and_ext = f"{suffix}{ext}"
        max_base_len = TELEGRAM_FILENAME_LIMIT - len(full_suffix_and_ext)
        
        if len(base_name) > max_base_len:
            base_name = base_name[:max_base_len].rstrip()
        
        new_filename = f"{base_name}{full_suffix_and_ext}"
        new_path = os.path.join(directory, new_filename)
        
        # Rename the file
        os.rename(file_path, new_path)
        return new_path
    except Exception as e:
        logging.error(f"Error adding suffix to filename: {e}")
        return file_path

async def keep_alive():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await session.get(KEEP_ALIVE_URL)
                logging.info("Sent keep-alive request.")
            except Exception as e:
                logging.error(f"Keep-alive request failed: {e}")
            await asyncio.sleep(111)

# ------------------ Bot Class ------------------
class MN_Bot(Client):
    MAX_MSG_LENGTH = 4000
    DOWNLOAD_STATUS_UPDATE_INTERVAL = 5 # seconds

    def __init__(self):
        super().__init__(
            "MN-Bot",
            api_id=API.ID,
            api_hash=API.HASH,
            bot_token=BOT.TOKEN,
            workers=16,
        )
        self.channel_id = CHANNEL.ID
        self.owner_id = OWNER.ID
        self.posted = set()

        self.ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
        self.ses.add_dht_router('router.bittorrent.com', 6881)
        self.ses.add_dht_router('router.utorrent.com', 6881)
        self.ses.add_dht_router('dht.transmissionbt.com', 6881)
        self.ses.add_dht_router('dht.libtorrent.org', 6881)
        self.ses.start_dht()
        logging.info("Libtorrent session initialized.")

        self.active_downloads = {} # {info_hash: {'handle': torrent_handle, 'message': pyrogram_message_object}}
        self.download_queue = asyncio.Queue() # For managing new downloads sequentially

    async def _add_torrent_to_session(self, magnet_link: str, entry_title: str):
        """Adds a magnet link to the libtorrent session and starts downloading."""
        try:
            params = {
                'save_path': DOWNLOAD_DIR,
                'storage_mode': lt.storage_mode_t.storage_mode_sparse,
                'paused': False,
                'auto_managed': True,
                'duplicate_is_error': False
            }
            handle = lt.add_magnet_uri(self.ses, magnet_link, params)
            
            logging.info(f"Fetching metadata for {entry_title}...")
            start_time = time.time()
            
            # Polling libtorrent for metadata with timeout
            while not handle.has_metadata():
                self.ses.wait_for_alerts(1000) # Wait for 1 second for alerts
                if time.time() - start_time > MAGNET_RESPONSE_TIMEOUT_SECONDS:
                    logging.warning(f"Timeout ({MAGNET_RESPONSE_TIMEOUT_SECONDS}s) fetching metadata for {entry_title}. Skipping download.")
                    await self.send_message(self.owner_id, 
                                            f"🚫 **Skipped:** `{entry_title}`\n"
                                            f"Magnet link did not respond within {MAGNET_RESPONSE_TIMEOUT_SECONDS} seconds.")
                    self.ses.remove_torrent(handle)
                    return None

            torrent_info = handle.torrent_file()
            total_size_bytes = torrent_info.total_size()
            total_size_mb = total_size_bytes / (1024 * 1024)

            if total_size_mb > MAX_FILE_SIZE_MB:
                logging.info(f"Skipping download of '{entry_title}' due to large size ({total_size_mb:.2f} MB).")
                await self.send_message(self.owner_id, 
                                        f"🚫 **Skipped:** `{entry_title}`\n"
                                        f"Size ({total_size_mb:.2f} MB) exceeds maximum allowed ({MAX_FILE_SIZE_MB} MB).")
                self.ses.remove_torrent(handle)
                return None
            
            initial_message_text = f"**Starting Download:** `{entry_title}`\n" \
                                   f"**Total Size:** `{total_size_mb:.2f} MB`\n" \
                                   "**Progress:** `0.00%`\n" \
                                   "**Downloaded:** `0.00 MB`\n" \
                                   "**Speed:** `0.00 KB/s`\n" \
                                   "**Peers:** `0 (0)`"
            
            status_message = await self.send_message(self.owner_id, initial_message_text)
            
            self.active_downloads[handle.info_hash()] = {
                'handle': handle,
                'message': status_message,
                'entry_title': entry_title,
                'total_size_bytes': total_size_bytes,
                'last_update_time': time.time()
            }
            logging.info(f"Added '{entry_title}' for download. Infohash: {handle.info_hash()}")
            return handle
        except Exception as e:
            logging.error(f"Error adding torrent for {entry_title}: {e}")
            await self.send_message(self.owner_id, 
                                    f"❌ **Failed to Start Download:** `{entry_title}`\n"
                                    f"Error: `{e}`")
            if 'handle' in locals() and handle.is_valid():
                self.ses.remove_torrent(handle)
            return None

    async def _update_download_progress(self):
        """Periodically updates the download progress messages in owner's PM."""
        while True:
            await asyncio.sleep(self.DOWNLOAD_STATUS_UPDATE_INTERVAL)
            
            # Create a copy to iterate because items might be removed during iteration
            for info_hash, data in list(self.active_downloads.items()):
                handle = data['handle']
                status_message = data['message']
                entry_title = data['entry_title']
                total_size_bytes = data['total_size_bytes']
                
                s = handle.status()
                
                # Check for completion
                if s.state == lt.torrent_status.seeding:
                    logging.info(f"Download of '{entry_title}' completed! ({s.total_done / (1024*1024):.2f} MB)")
                    
                    final_message_text = f"✅ **Download Completed:** `{entry_title}`\n" \
                                         f"**Total Size:** `{s.total_done / (1024*1024):.2f} MB`\n" \
                                         f"**Time Taken:** `{str(datetime.now() - datetime.fromtimestamp(handle.status().added_time)).split('.')[0]}`"
                    try:
                        await status_message.edit_text(final_message_text)
                    except errors.MessageNotModified:
                        pass
                    except Exception as e:
                        logging.error(f"Error editing final message for {entry_title}: {e}")

                    await self._send_downloaded_file_to_channel(handle, entry_title)
                    
                    del self.active_downloads[info_hash]
                    self.ses.remove_torrent(handle)
                    continue
                
                # Check for errors
                if s.has_error:
                    logging.error(f"Error downloading '{entry_title}': {s.error}")
                    error_message_text = f"❌ **Download Failed:** `{entry_title}`\n" \
                                         f"**Error:** `{s.error}`"
                    try:
                        await status_message.edit_text(error_message_text)
                    except errors.MessageNotModified:
                        pass
                    except Exception as e:
                        logging.error(f"Error editing error message for {entry_title}: {e}")
                    
                    del self.active_downloads[info_hash]
                    self.ses.remove_torrent(handle)
                    continue
                
                # Update progress
                progress = s.progress * 100
                downloaded_mb = s.total_done / (1024 * 1024)
                download_speed_kbps = s.download_rate / 1024
                
                progress_bar = self._create_progress_bar(progress)
                
                updated_message_text = f"**Downloading:** `{entry_title}`\n" \
                                       f"**Total Size:** `{total_size_bytes / (1024*1024):.2f} MB`\n" \
                                       f"**Progress:** `{progress:.2f}%` {progress_bar}\n" \
                                       f"**Downloaded:** `{downloaded_mb:.2f} MB`\n" \
                                       f"**Speed:** `{download_speed_kbps:.2f} KB/s`\n" \
                                       f"**Peers:** `{s.num_peers} (connected: {s.num_peers})`" 

                try:
                    if time.time() - data['last_update_time'] > self.DOWNLOAD_STATUS_UPDATE_INTERVAL or abs(s.progress - handle.status().progress) > 0.01:
                        await status_message.edit_text(updated_message_text)
                        data['last_update_time'] = time.time()
                except errors.MessageNotModified:
                    pass
                except Exception as e:
                    logging.error(f"Error editing message for {entry_title}: {e}")

    def _create_progress_bar(self, progress: float, bar_length: int = 20) -> str:
        filled_length = int(bar_length * progress // 100)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        return f"[{bar}]"

    async def _send_downloaded_file_to_channel(self, handle: lt.torrent_handle, entry_title: str):
        try:
            torrent_info = handle.torrent_file()
            
            files = []
            for i in range(torrent_info.num_files()):
                f = torrent_info.file_at(i)
                files.append({'path': f.path, 'size': f.size})
            
            if not files:
                logging.warning(f"No files found in torrent for {entry_title}.")
                return

            files.sort(key=lambda x: x['size'], reverse=True)
            largest_file = files[0]
            # Construct the full path carefully, considering subdirectories in torrents
            file_path = os.path.join(DOWNLOAD_DIR, largest_file['path'])

            if not os.path.exists(file_path):
                logging.error(f"Downloaded file not found at: {file_path}. Path: {file_path}")
                await self.send_message(self.owner_id, f"❌ Downloaded file not found on disk for '{entry_title}'. Path: `{file_path}`")
                return

            if largest_file['size'] > 2 * 1024 * 1024 * 1024:
                 logging.warning(f"File '{largest_file['path']}' is too large ({largest_file['size'] / (1024*1024*1024):.2f} GB) for Telegram (max 2GB). Skipping upload.")
                 await self.send_message(self.owner_id, f"⚠️ Downloaded file '{entry_title}' is too large to upload to Telegram ({largest_file['size'] / (1024*1024*1024):.2f} GB).")
                 
                 # Still remove the local file
                 os.remove(file_path)
                 logging.info(f"Removed large downloaded file: {file_path}")
                 return

            # Add suffix to filename
            file_path = add_suffix_to_filename(file_path)
            
            # Generate thumbnail for video files
            thumbnail_path = None
            file_ext = os.path.splitext(file_path)[1].lower()
            video_extensions = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v']
            
            if file_ext in video_extensions:
                thumbnail_name = f"{os.path.splitext(os.path.basename(file_path))[0]}_thumb.jpg"
                thumbnail_path = os.path.join(THUMBNAIL_DIR, thumbnail_name)
                
                generated_thumb = generate_thumbnail(file_path, thumbnail_path)
                if not generated_thumb:
                    thumbnail_path = create_default_thumbnail()
            
            logging.info(f"Uploading '{os.path.basename(file_path)}' ({largest_file['size'] / (1024*1024):.2f} MB) to channel as document...")
            
            # Send as document with thumbnail (videos are sent as files, not video messages)
            await self.send_document(
                chat_id=self.channel_id,
                document=file_path,
                caption=f"**{entry_title}**\n\n_Downloaded by bot._",
                thumb=thumbnail_path if thumbnail_path and os.path.exists(thumbnail_path) else None,
                force_document=True
            )
            logging.info(f"Successfully uploaded '{os.path.basename(file_path)}' to channel as document.")

            # Clean up files
            os.remove(file_path)
            logging.info(f"Removed downloaded file: {file_path}")
            
            if thumbnail_path and os.path.exists(thumbnail_path) and "default_thumb.jpg" not in thumbnail_path:
                os.remove(thumbnail_path)
                logging.info(f"Removed thumbnail: {thumbnail_path}")

        except Exception as e:
            logging.error(f"Error sending downloaded file to channel for {entry_title}: {e}")
            await self.send_message(self.owner_id, f"❌ Failed to upload '{entry_title}' to channel. Error: {e}")

    async def send_safe(self, chat_id, content, content_type, entry):
        if content_type == 'link' and "magnet:" in content:
            await self.download_queue.put((content, entry.title))
            logging.info(f"Added '{entry.title}' to download queue.")
            await self.send_message(self.owner_id, f"📥 Queued for download: `{entry.title}`")

        elif content_type == 'torrent':
            try:
                resp = requests.get(content, timeout=10)
                resp.raise_for_status()
                torrent_file_path = os.path.join(DOWNLOAD_DIR, f"{entry.title}.torrent")
                with open(torrent_file_path, 'wb') as f:
                    f.write(resp.content)
                
                # Add to download queue for sequential processing
                await self.download_queue.put((torrent_file_path, entry.title))
                logging.info(f"Added .torrent file '{entry.title}' to download queue.")
                await self.send_message(self.owner_id, f"📥 Queued for download (from .torrent): `{entry.title}`")

            except Exception as e:
                logging.error(f"Failed to fetch .torrent file {content}: {e}")
                await self.send_message(self.owner_id, f"❌ Failed to fetch .torrent file '{entry.title}'. Error: {e}")
        else:
            logging.info(f"Skipping unsupported link type for '{entry.title}': {content}")

    async def process_psa_feed(self, url):
        feed = fetch_feed(url)
        for entry in feed.entries:
            if entry.link in self.posted:
                continue
            
            if not is_valid_entry(entry):
                continue
            
            content, kind = download_torrent_or_link(entry)
            
            # This now queues the download, instead of starting it directly
            await self.send_safe(self.owner_id, content, kind, entry)
            self.posted.add(entry.link)
            await asyncio.sleep(2)

    async def initial_post(self):
        items = []
        for url in PSA_FEEDS:
            feed = fetch_feed(url)
            for e in feed.entries:
                if not is_valid_entry(e):
                    continue
                
                t = e.get("published_parsed") or e.get("updated_parsed")
                dt = datetime.fromtimestamp(time.mktime(t)) if t else None
                items.append((dt, e))
        
        recent = sorted(
            [i for i in items if i[0]],
            key=lambda x: x[0],
            reverse=True
        )[:10]
        
        for _, entry in recent:
            if entry.link in self.posted:
                continue
            content, kind = download_torrent_or_link(entry)
            await self.send_safe(self.owner_id, content, kind, entry)
            self.posted.add(entry.link)
            await asyncio.sleep(2)

    async def _download_worker(self):
        """Worker to process download queue. Only one worker for sequential processing."""
        while True:
            # Get the item from the queue
            item = await self.download_queue.get()
            content, entry_title = item

            logging.info(f"Processing download from queue for: {entry_title}")
            
            if content.startswith("magnet:"):
                # It's a magnet link
                await self._add_torrent_to_session(content, entry_title)
            elif content.endswith(".torrent") and os.path.exists(content):
                # It's a path to a local .torrent file
                try:
                    params = {
                        'save_path': DOWNLOAD_DIR,
                        'storage_mode': lt.storage_mode_t.storage_mode_sparse,
                        'paused': False,
                        'auto_managed': True,
                        'duplicate_is_error': False
                    }
                    handle = self.ses.add_torrent(params)
                    # Read torrent file content
                    with open(content, 'rb') as f:
                        torrent_file_content = f.read()
                    handle.set_torrent_file(lt.torrent_info(lt.bdecode(torrent_file_content))) # Load torrent info
                    
                    # Wait for metadata (can be faster for .torrent files as info is already there)
                    logging.info(f"Loading metadata for {entry_title} from .torrent file...")
                    start_time = time.time()
                    while not handle.has_metadata():
                        self.ses.wait_for_alerts(100) # Shorter wait here, metadata should be almost instant
                        if time.time() - start_time > 10: # A short timeout for .torrent metadata loading
                            logging.warning(f"Timeout loading metadata from .torrent file for {entry_title}. Skipping download.")
                            await self.send_message(self.owner_id, 
                                                    f"🚫 **Skipped:** `{entry_title}`\n"
                                                    f"Failed to load metadata from .torrent file.")
                            self.ses.remove_torrent(handle)
                            os.remove(content) # Clean up the local .torrent file
                            self.download_queue.task_done()
                            continue # Move to next item in queue

                    torrent_info = handle.torrent_file()
                    total_size_bytes = torrent_info.total_size()
                    total_size_mb = total_size_bytes / (1024 * 1024)

                    if total_size_mb > MAX_FILE_SIZE_MB:
                        logging.info(f"Skipping download of '{entry_title}' due to large size ({total_size_mb:.2f} MB).")
                        await self.send_message(self.owner_id, 
                                                f"🚫 **Skipped:** `{entry_title}`\n"
                                                f"Size ({total_size_mb:.2f} MB) exceeds maximum allowed ({MAX_FILE_SIZE_MB} MB).")
                        self.ses.remove_torrent(handle)
                        os.remove(content)
                        self.download_queue.task_done()
                        continue
                    
                    initial_message_text = f"**Starting Download (Torrent File):** `{entry_title}`\n" \
                                           f"**Total Size:** `{total_size_mb:.2f} MB`\n" \
                                           "**Progress:** `0.00%`\n" \
                                           "**Downloaded:** `0.00 MB`\n" \
                                           "**Speed:** `0.00 KB/s`\n" \
                                           "**Peers:** `0 (0)`"
                    
                    status_message = await self.send_message(self.owner_id, initial_message_text)
                    
                    self.active_downloads[handle.info_hash()] = {
                        'handle': handle,
                        'message': status_message,
                        'entry_title': entry_title,
                        'total_size_bytes': total_size_bytes,
                        'last_update_time': time.time()
                    }
                    logging.info(f"Added .torrent '{entry_title}' for download. Infohash: {handle.info_hash()}")
                    os.remove(content) # Clean up the .torrent file after successfully adding it to session
                except Exception as e:
                    logging.error(f"Error processing .torrent file {content}: {e}")
                    await self.send_message(self.owner_id, 
                                            f"❌ **Failed to Process .torrent File:** `{entry_title}`\n"
                                            f"Error: `{e}`")
                    if os.path.exists(content):
                        os.remove(content)
            else:
                logging.warning(f"Unknown item in download queue: {item}")
            
            # Mark the task as done so the queue knows to continue
            self.download_queue.task_done()

    async def auto_post(self):
        while True:
            try:
                for url in PSA_FEEDS:
                    await self.process_psa_feed(url)
                    await asyncio.sleep(5)
                await asyncio.sleep(300) # 5 minutes
            except Exception as e:
                logging.error(f"Error in auto_post: {e}")
                await asyncio.sleep(60)

    async def start(self):
        await super().start()
        me = await self.get_me()
        BOT.USERNAME = f"@{me.username}"
        
        asyncio.create_task(keep_alive())
        asyncio.create_task(self._update_download_progress())
        
        # Start only ONE download worker for sequential processing
        asyncio.create_task(self._download_worker()) 

        await self.initial_post()
        asyncio.create_task(self.auto_post())
        await self.send_message(self.owner_id, f"{me.first_name} ✅ Bot started and torrent monitoring active. Processing downloads sequentially.")

    async def stop(self, *args):
        logging.info("Stopping all active torrents...")
        for info_hash, data in self.active_downloads.items():
            try:
                data['handle'].pause()
                self.ses.remove_torrent(data['handle'])
            except Exception as e:
                logging.error(f"Error pausing/removing torrent {info_hash}: {e}")
        self.ses = None
        await super().stop()
        logging.info("Bot stopped")

# ------------------ Main ------------------
if __name__ == "__main__":
    threading.Thread(target=run_flask).start()
    MN_Bot().run()
