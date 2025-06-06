import asyncio
import feedparser
import json
import logging
import os
import re
import subprocess
import sys
import time
import threading
from datetime import datetime, timedelta # Ensure timedelta is imported
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import aiohttp
import aria2p
from flask import Flask
from PIL import Image
from pymongo import MongoClient
from pyrogram import Client, errors, idle
from config import BOT, API, OWNER

# ------------------ Constants ------------------
KEEP_ALIVE_URL = "https://abundant-barbie-musammiln-db578527.koyeb.app/"
PSA_FEEDS = ["https://bt4gprx.com/search?q=psa&page=rss"]
MAX_FILE_SIZE_MB = 1900
DOWNLOAD_STATUS_UPDATE_INTERVAL = 10
TELEGRAM_FILENAME_LIMIT = 60
SUFFIX = " -@MNTGX.-"
THUMBNAIL_URL = "https://i.ibb.co/MDwd1f3D/6087047735061627461.jpg"
MONGODB_URI = "mongodb+srv://mntgx:mntgx@cluster0.pzcpq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# ------------------ Database Setup ------------------
class MongoDB:
    def __init__(self):
        try:
            self.client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            self.db = self.client.torrent_bot
            self.completed = self.db.completed
            self.queue = self.db.queue
            self.active = self.db.active
            # Test connection
            self.client.server_info()
            logging.info("MongoDB connected successfully")
        except Exception as e:
            logging.error(f"MongoDB connection failed: {e}")

    def add_to_queue(self, content: str, title: str):
        try:
            self.queue.insert_one({
                "content": content,
                "title": title,
                "added_at": datetime.utcnow()
            })
        except Exception as e:
            logging.error(f"Failed to add to queue: {e}")

    def move_to_active(self, gid: str, content: str, title: str):
        try:
            self.queue.delete_one({"content": content})
            self.active.insert_one({
                "gid": gid,
                "content": content,
                "title": title,
                "started_at": datetime.utcnow()
            })
        except Exception as e:
            logging.error(f"Failed to move to active: {e}")

    def move_to_completed(self, gid: str):
        try:
            active = self.active.find_one({"gid": gid})
            if active:
                self.active.delete_one({"gid": gid})
                self.completed.insert_one({
                    **active,
                    "completed_at": datetime.utcnow()
                })
        except Exception as e:
            logging.error(f"Failed to move to completed: {e}")

    def get_queued_items(self):
        try:
            return list(self.queue.find())
        except Exception as e:
            logging.error(f"Failed to get queued items: {e}")
            return []

    def get_active_downloads(self):
        try:
            return list(self.active.find())
        except Exception as e:
            logging.error(f"Failed to get active downloads: {e}")
            return []

    def is_completed(self, content: str):
        try:
            return bool(self.completed.find_one({"content": content}))
        except Exception as e:
            logging.error(f"Failed to check completion: {e}")
            return False

    def cleanup_stale_active(self):
        """Remove stale active downloads"""
        try:
            # Remove active downloads older than 24 hours
            cutoff = datetime.utcnow() - timedelta(hours=24)
            result = self.active.delete_many({"started_at": {"$lt": cutoff}})
            if result.deleted_count > 0:
                logging.info(f"Cleaned up {result.deleted_count} stale active downloads")
        except Exception as e:
            logging.error(f"Failed to cleanup stale active: {e}")

# ------------------ Path Setup ------------------
BASE_DIR = Path(__file__).parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
THUMBNAIL_DIR = BASE_DIR / "thumbnails"

DOWNLOAD_DIR.mkdir(exist_ok=True)
THUMBNAIL_DIR.mkdir(exist_ok=True)

# ------------------ Logging Setup ------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# ------------------ Flask App ------------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Torrent Bot is running!"

@app.route('/health')
def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

def run_flask():
    try:
        app.run(host='0.0.0.0', port=8000, debug=False, use_reloader=False)
    except Exception as e:
        logging.error(f"Flask server error: {e}")

# ------------------ Missing Functions ------------------
def fetch_feed(url):
    """Fetch RSS feed"""
    try:
        feed = feedparser.parse(url)
        logging.info(f"Fetched {len(feed.entries)} entries from {url}")
        return feed
    except Exception as e:
        logging.error(f"Failed to fetch feed {url}: {e}")
        return feedparser.FeedParserDict()

def download_torrent_or_link(entry):
    """Extract torrent/magnet link from RSS entry"""
    try:
        # Check for magnet link in entry
        if hasattr(entry, 'link') and entry.link.startswith('magnet:'):
            return entry.link, 'magnet'
        
        # Check for torrent file in enclosures
        if hasattr(entry, 'enclosures'):
            for enclosure in entry.enclosures:
                if enclosure.type == 'application/x-bittorrent':
                    return enclosure.href, 'torrent'
        
        # Check for links in entry content
        if hasattr(entry, 'links'):
            for link in entry.links:
                if link.type == 'application/x-bittorrent':
                    return link.href, 'torrent'
                elif 'magnet:' in link.href:
                    return link.href, 'magnet'
        
        # Check in description for magnet links
        if hasattr(entry, 'description'):
            magnet_match = re.search(r'magnet:\?[^\s<>"]*', entry.description)
            if magnet_match:
                return magnet_match.group(0), 'magnet'
        
        # Fallback to entry link
        return entry.link, 'link'
        
    except Exception as e:
        logging.error(f"Failed to extract torrent link: {e}")
        return entry.link, 'link'

# ------------------ Aria2 Setup ------------------
async def start_aria2():
    """Start aria2 daemon"""
    process = None
    api = None
    try:
        # Kill any existing aria2 processes
        try:
            # Use 'killall' instead of 'pkill' for broader compatibility and a more direct kill
            subprocess.run(["killall", "-q", "aria2c"], check=False, timeout=5)
            await asyncio.sleep(2) # Give some time for processes to terminate
        except Exception as e:
            logging.warning(f"Failed to kill existing aria2 processes (might not be running): {e}")
        
        # Start aria2 daemon
        # Temporarily removing stderr=subprocess.DEVNULL for debugging purposes
        process = subprocess.Popen([
            "aria2c",
            "--enable-rpc",
            "--rpc-listen-all=true",
            "--rpc-listen-port=6800",
            "--rpc-allow-origin-all=true",
            f"--dir={DOWNLOAD_DIR}",
            "--max-concurrent-downloads=2",
            "--max-connection-per-server=8",
            "--min-split-size=1M",
            "--split=8",
            "--bt-max-peers=50",
            "--seed-time=0",
            "--daemon=true",
            "--quiet=true"
        ], stdout=subprocess.DEVNULL) # Keep stderr visible by not redirecting it
        
        # Wait for aria2 to start
        await asyncio.sleep(5) # Increased initial sleep
        
        # Test connection
        for i in range(10):
            try:
                api = aria2p.API(aria2p.Client(host="http://localhost", port=6800, secret=""))
                api.get_version() # This will raise an exception if connection fails
                logging.info("Aria2 started successfully")
                return process, api
            except Exception as e:
                logging.warning(f"Attempt {i+1}/10 to connect to aria2 failed: {e}")
                await asyncio.sleep(3) # Increased sleep between retries
        
        raise Exception("Failed to connect to aria2 after 10 attempts")
        
    except Exception as e:
        logging.error(f"Failed to start aria2: {e}")
        if process: # If process was started but failed later, try to terminate it
            try:
                process.terminate()
            except Exception as term_e:
                logging.error(f"Failed to terminate aria2 process: {term_e}")
        return None, None

# ------------------ Helper Functions ------------------
def add_suffix(filename: str) -> str:
    """Add suffix to filename while respecting Telegram limits"""
    base, ext = os.path.splitext(filename)
    new_base = f"{base}{SUFFIX}"
    if len(new_base) + len(ext) > TELEGRAM_FILENAME_LIMIT:
        max_base_len = TELEGRAM_FILENAME_LIMIT - len(ext) - len(SUFFIX)
        new_base = f"{base[:max_base_len].rstrip()}{SUFFIX}"
    return f"{new_base}{ext}"

async def download_thumbnail() -> Optional[Path]:
    """Download thumbnail from URL"""
    thumb_path = THUMBNAIL_DIR / "thumbnail.jpg"
    if thumb_path.exists():
        return thumb_path
    
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(THUMBNAIL_URL) as resp:
                if resp.status == 200:
                    with open(thumb_path, 'wb') as f:
                        f.write(await resp.read())
                    return thumb_path
    except Exception as e:
        logging.error(f"Thumbnail download failed: {e}")
        return None

def create_default_thumbnail() -> Optional[Path]:
    """Create a default thumbnail"""
    thumb_path = THUMBNAIL_DIR / "default.jpg"
    try:
        img = Image.new('RGB', (320, 240), color=(25, 25, 25))
        # Add some text
        try:
            from PIL import ImageDraw, ImageFont
            draw = ImageDraw.Draw(img)
            # You might need to specify a font path if default fonts are not available
            # font = ImageFont.truetype("arial.ttf", 20) # Example: If Arial is available
            # draw.text((50, 100), "MNTGX", fill=(255, 255, 255), font=font)
            draw.text((50, 100), "MNTGX", fill=(255, 255, 255))
        except ImportError:
            logging.warning("Pillow with ImageDraw/ImageFont not fully available. Default thumbnail text skipped.")
        except Exception as font_e:
            logging.warning(f"Failed to add text to thumbnail: {font_e}")
        img.save(thumb_path, "JPEG", quality=80)
        return thumb_path
    except Exception as e:
        logging.error(f"Failed to create default thumb: {e}")
        return None

async def get_thumbnail() -> Optional[Path]:
    """Get thumbnail (download or create default)"""
    thumb = await download_thumbnail()
    return thumb or create_default_thumbnail()

# ------------------ Bot Class ------------------
class TorrentBot(Client):
    def __init__(self):
        super().__init__(
            "TorrentBot",
            api_id=API.ID,
            api_hash=API.HASH,
            bot_token=BOT.TOKEN,
            workers=8
        )
        self.owner_id = OWNER.ID
        self.db = MongoDB()
        self.download_queue = asyncio.Queue()
        self.active_downloads = {}
        self.thumbnail_path = None
        self.aria2 = None
        self.aria2_process = None
        self.running = False

    async def _rename_with_suffix(self, file_path: Path) -> Path:
        """Rename file with suffix"""
        try:
            new_name = add_suffix(file_path.name)
            new_path = file_path.with_name(new_name)
            if file_path.exists():
                file_path.rename(new_path)
            return new_path
        except Exception as e:
            logging.error(f"Renaming failed: {e}")
            return file_path

    async def _get_largest_file(self, download) -> Optional[Path]:
        """Get the largest file from download"""
        try:
            if not download.files:
                logging.warning(f"No files found for download GID: {download.gid}")
                return None
            
            largest_file = max(download.files, key=lambda f: f.length)
            file_path = Path(largest_file.path)
            
            # Check if file exists and has reasonable size
            if file_path.exists() and file_path.stat().st_size > 1024:  # At least 1KB
                return file_path
            
            # If largest file doesn't exist, try to find any existing file
            for file in download.files:
                file_path = Path(file.path)
                if file_path.exists() and file_path.stat().st_size > 1024:
                    logging.info(f"Using alternate file {file_path.name} as largest was not found/valid.")
                    return file_path
            
            logging.warning(f"No valid downloadable file found for GID: {download.gid}")
            return None
            
        except Exception as e:
            logging.error(f"Error getting largest file for {download.gid}: {e}")
            return None

    async def _upload_file(self, download):
        """Upload completed file to Telegram"""
        try:
            file_path = await self._get_largest_file(download)
            if not file_path:
                raise FileNotFoundError(f"No valid downloaded file found for GID: {download.gid}")
            
            file_size = file_path.stat().st_size
            if file_size > MAX_FILE_SIZE_MB * 1024 * 1024:
                raise ValueError(f"File too large: {file_size / (1024*1024):.2f}MB (Max: {MAX_FILE_SIZE_MB}MB)")
            
            if file_size < 1024:  # Less than 1KB
                raise ValueError(f"File too small ({file_size} bytes), possibly corrupted: {file_path.name}")
            
            file_path = await self._rename_with_suffix(file_path)
            
            if not self.thumbnail_path:
                self.thumbnail_path = await get_thumbnail()
            
            caption = (
                f"**{file_path.stem.replace(SUFFIX, '')}**\n\n"
                f"📁 Size: {file_size / (1024*1024):.2f} MB\n"
                f"🔗 Source: RSS Feed\n"
                f"⚡ Powered by @MNTGX"
            )
            
            logging.info(f"Attempting to upload {file_path.name} to Telegram...")
            await self.send_document(
                chat_id=self.owner_id,
                document=str(file_path),
                caption=caption,
                thumb=str(self.thumbnail_path) if self.thumbnail_path else None,
                progress=self._upload_progress
            )
            logging.info(f"Successfully uploaded {file_path.name}")
            
            # Clean up file after successful upload
            try:
                file_path.unlink()
                # Also clean up parent directory if empty
                if file_path.parent != DOWNLOAD_DIR and not any(file_path.parent.iterdir()):
                    file_path.parent.rmdir()
                    logging.info(f"Removed empty directory: {file_path.parent}")
            except Exception as cleanup_e:
                logging.warning(f"Failed to clean up file {file_path} or its directory: {cleanup_e}")
            
            self.db.move_to_completed(download.gid)
            return True
            
        except Exception as e:
            logging.error(f"Upload failed for GID {download.gid}: {e}")
            await self.send_message(
                self.owner_id,
                f"❌ **Upload Failed**\n"
                f"📁 File: {getattr(download, 'name', 'Unknown')}\n"
                f"🚫 Error: {str(e)[:200]}" # Limit error message length
            )
            return False

    async def _upload_progress(self, current, total):
        """Upload progress callback"""
        try:
            percent = current * 100 / total
            # Log at 0%, 20%, 40%, 60%, 80%, 100%
            if int(percent) % 20 == 0 or percent == 100 or percent == 0:
                # Use a simple threshold to avoid too many logs
                if not hasattr(self, '_last_logged_upload_percent'):
                    self._last_logged_upload_percent = -1
                if int(percent / 20) > int(self._last_logged_upload_percent / 20) or percent == 100:
                    logging.info(f"Upload progress: {percent:.1f}%")
                    self._last_logged_upload_percent = percent
        except Exception as e:
            logging.debug(f"Upload progress logging error: {e}") # Debug level as this is minor

    async def _process_queue(self):
        """Process items from database queue"""
        try:
            queued = self.db.get_queued_items()
            logging.info(f"Processing {len(queued)} queued items from DB")
            for item in queued:
                # Add to asyncio queue if not already active or completed
                if not self.db.is_completed(item['content']) and item['gid'] not in self.active_downloads:
                    await self.download_queue.put((item['content'], item['title']))
                else:
                    logging.info(f"Skipping DB queued item '{item['title']}' - already completed or active.")
        except Exception as e:
            logging.error(f"Error processing queue from DB: {e}")

    async def _recover_active(self):
        """Recover active downloads on restart"""
        try:
            # Clean up stale downloads first
            self.db.cleanup_stale_active()
            
            active = self.db.get_active_downloads()
            logging.info(f"Recovering {len(active)} active downloads from DB")
            
            for item in active:
                try:
                    # Check if aria2 instance is available
                    if not self.aria2:
                        logging.warning(f"Aria2 not available, cannot recover {item['title']}. Adding to queue.")
                        await self.download_queue.put((item['content'], item['title']))
                        self.db.active.delete_one({"gid": item['gid']}) # Remove from active if aria2 not up
                        continue

                    download = self.aria2.get_download(item['gid'])
                    if download.is_complete:
                        logging.info(f"Found completed download in aria2: {item['title']}")
                        await self._upload_file(download)
                    elif download.is_active:
                        self.active_downloads[download.gid] = {
                            'title': item['title'],
                            'start_time': item['started_at'].timestamp() if 'started_at' in item else time.time()
                        }
                        logging.info(f"Recovered active download: {item['title']}")
                    elif download.status in ["paused", "waiting"]: # Consider resuming if paused/waiting
                        logging.info(f"Found {download.status} download in aria2: {item['title']}, resuming...")
                        download.resume()
                        self.active_downloads[download.gid] = {
                            'title': item['title'],
                            'start_time': item['started_at'].timestamp() if 'started_at' in item else time.time()
                        }
                    else: # If status is error, removed, etc.
                        self.db.active.delete_one({"gid": item['gid']})
                        logging.info(f"Removed stale/inactive download from DB: {item['title']} (status: {download.status})")
                        
                except aria2p.ClientException as e:
                    if "not found" in str(e).lower():
                        # GID not found in aria2, remove from database and requeue
                        logging.info(f"GID {item['gid']} not found in aria2. Re-adding '{item['title']}' to queue.")
                        self.db.active.delete_one({"gid": item['gid']})
                        await self.download_queue.put((item['content'], item['title'])) # Requeue it
                    else:
                        logging.error(f"Aria2 Client error recovering download {item['gid']}: {e}")
                except Exception as e:
                    logging.error(f"Error recovering download {item['gid']}: {e}")
                    
        except Exception as e:
            logging.error(f"Error recovering active downloads overall: {e}")

    async def _download_worker(self):
        """Worker to process download queue"""
        while self.running:
            try:
                # Wait for item with timeout to allow checking running status
                try:
                    content, title = await asyncio.wait_for(
                        self.download_queue.get(), timeout=5.0
                    )
                except asyncio.TimeoutError:
                    await asyncio.sleep(1) # Small sleep to prevent busy-waiting
                    continue
                
                # Check if already completed or active (due to recovery/concurrent adds)
                if self.db.is_completed(content):
                    logging.info(f"Skipping already completed: {title}")
                    self.download_queue.task_done()
                    continue
                
                # Check if already in active_downloads (could happen during recovery)
                is_already_active = False
                for active_dl in self.active_downloads.values():
                    if active_dl['title'] == title: # Basic check for title match
                        is_already_active = True
                        break
                
                if is_already_active:
                    logging.info(f"Skipping already active: {title}")
                    self.download_queue.task_done()
                    continue

                # Ensure aria2 is available before adding a download
                if not self.aria2:
                    logging.error(f"Aria2 is not initialized, cannot start download: {title}. Requeuing.")
                    await self.download_queue.put((content, title)) # Put back in queue
                    self.download_queue.task_done()
                    await asyncio.sleep(10) # Wait before retrying
                    continue

                # Add download to aria2
                try:
                    logging.info(f"Attempting to add download to aria2: {title}")
                    if content.startswith('magnet:'):
                        download = self.aria2.add_magnet(content, {"dir": str(DOWNLOAD_DIR)})
                    else:
                        download = self.aria2.add_uri([content], {"dir": str(DOWNLOAD_DIR)}) # add_uri for direct links
                    
                    self.db.move_to_active(download.gid, content, title)
                    self.active_downloads[download.gid] = {
                        'title': title,
                        'start_time': time.time()
                    }
                    
                    await self.send_message(
                        self.owner_id,
                        f"📥 **Download Started**\n"
                        f"📁 {title[:50]}{'...' if len(title) > 50 else ''}\n"
                        f"🆔 GID: `{download.gid}`"
                    )
                    
                    logging.info(f"Started download: {title} (GID: {download.gid})")
                    
                except Exception as e:
                    logging.error(f"Download start failed for '{title}': {e}")
                    await self.send_message(
                        self.owner_id,
                        f"❌ **Download Failed to Start**\n"
                        f"📁 {title[:50]}{'...' if len(title) > 50 else ''}\n"
                        f"🚫 Error: {str(e)[:200]}"
                    )
                finally:
                    self.download_queue.task_done()
                    
            except Exception as e:
                logging.error(f"Download worker main loop error: {e}")
                await asyncio.sleep(5)

    async def _monitor_downloads(self):
        """Monitor active downloads"""
        while self.running:
            try:
                await asyncio.sleep(DOWNLOAD_STATUS_UPDATE_INTERVAL)
                
                if not self.aria2:
                    logging.warning("Aria2 not available for monitoring. Skipping current monitoring cycle.")
                    await asyncio.sleep(5)
                    continue

                # It's safer to iterate on a copy of the dict if items might be deleted during iteration
                for gid in list(self.active_downloads.keys()): 
                    if not self.running: # Check if bot is stopping
                        break
                    try:
                        download = self.aria2.get_download(gid)
                        
                        if download.is_complete:
                            logging.info(f"Download completed in aria2: {self.active_downloads[gid]['title']}")
                            if await self._upload_file(download):
                                await self.send_message(
                                    self.owner_id,
                                    f"✅ **Upload Complete**\n"
                                    f"📁 {self.active_downloads[gid]['title'][:50]}{'...' if len(self.active_downloads[gid]['title']) > 50 else ''}"
                                )
                            if gid in self.active_downloads:
                                del self.active_downloads[gid]
                            
                        elif download.status == "error":
                            error_msg = download.error_message or "Unknown error"
                            logging.error(f"Download failed: {self.active_downloads[gid]['title']} - {error_msg}")
                            await self.send_message(
                                self.owner_id,
                                f"❌ **Download Failed**\n"
                                f"📁 {self.active_downloads[gid]['title'][:50]}{'...' if len(self.active_downloads[gid]['title']) > 50 else ''}\n"
                                f"🚫 Error: {error_msg[:200]}"
                            )
                            self.db.active.delete_one({"gid": gid})
                            if gid in self.active_downloads:
                                del self.active_downloads[gid]
                            
                        elif not download.is_active and download.status not in ["paused", "waiting", "complete"]:
                            # If it's not active, not paused, not waiting, and not complete, it's likely stuck or removed
                            logging.warning(f"Download {self.active_downloads[gid]['title']} is in unexpected state: {download.status}. Removing from active.")
                            self.db.active.delete_one({"gid": gid})
                            if gid in self.active_downloads:
                                del self.active_downloads[gid]

                        # Optionally, send progress updates (be careful not to spam)
                        # if download.is_active and time.time() - self.active_downloads[gid].get('last_update_time', 0) > 300: # Update every 5 min
                        #     progress_msg = f"⏳ **Downloading...**\n" \
                        #                    f"📁 {self.active_downloads[gid]['title'][:50]}{'...' if len(self.active_downloads[gid]['title']) > 50 else ''}\n" \
                        #                    f"📊 Progress: {download.progress:.2f}%\n" \
                        #                    f"⚡ Speed: {download.download_speed_string()}"
                        #     await self.send_message(self.owner_id, progress_msg)
                        #     self.active_downloads[gid]['last_update_time'] = time.time()

                    except aria2p.ClientException as e:
                        if "not found" in str(e).lower():
                            logging.info(f"Download GID {gid} not found in aria2 (likely finished/removed). Cleaning up DB.")
                            self.db.active.delete_one({"gid": gid})
                            if gid in self.active_downloads:
                                del self.active_downloads[gid]
                        else:
                            logging.error(f"Monitor error for {gid} (aria2 client error): {e}")
                    except Exception as e:
                        logging.error(f"Monitor error for {gid}: {e}")
                        
            except Exception as e:
                logging.error(f"Monitor loop error: {e}")
                await asyncio.sleep(30)

    async def process_feed(self, url):
        """Process RSS feed"""
        try:
            feed = fetch_feed(url)
            processed = 0
            
            for entry in feed.entries[:5]:  # Process 5 newest entries
                try:
                    if self.db.is_completed(entry.link):
                        logging.debug(f"Skipping RSS entry '{entry.title}' - already completed.")
                        continue
                    
                    content, link_type = download_torrent_or_link(entry)
                    if content and content != entry.link:  # Valid torrent/magnet found
                        # Check if already in queue or active to avoid duplicates
                        queued_titles = [item['title'] for item in self.db.get_queued_items()]
                        active_titles = [item['title'] for item in self.db.get_active_downloads()]

                        if entry.title in queued_titles or entry.title in active_titles:
                            logging.info(f"Skipping RSS entry '{entry.title}' - already queued or active.")
                            continue

                        self.db.add_to_queue(content, entry.title)
                        await self.download_queue.put((content, entry.title))
                        processed += 1
                        logging.info(f"Added '{entry.title}' to download queue.")
                        await asyncio.sleep(1)  # Rate limiting
                        
                except Exception as e:
                    logging.error(f"Error processing RSS entry '{entry.title}': {e}")
                    
            if processed > 0:
                logging.info(f"Added {processed} new downloads from feed: {url}")
                
        except Exception as e:
            logging.error(f"Error processing feed {url}: {e}")

    async def start(self):
        """Start the bot"""
        await super().start()
        self.running = True
        
        # Start aria2
        self.aria2_process, self.aria2 = await start_aria2()
        if not self.aria2:
            await self.send_message(self.owner_id, "❌ Failed to start aria2 daemon. Bot might not function correctly.")
            # Do not return here, allow bot to try and recover or at least respond to commands.
            # However, core functionality will be affected.
        
        # Get thumbnail
        self.thumbnail_path = await get_thumbnail()
        
        # Recover state
        await self._process_queue()
        await self._recover_active()
        
        # Start background tasks
        asyncio.create_task(self._download_worker())
        asyncio.create_task(self._monitor_downloads())
        asyncio.create_task(self._feed_loop())
        
        await self.send_message(
            self.owner_id, 
            f"✅ **Bot Started Successfully**\n\n"
            f"🔧 Features:\n"
            f"• MongoDB persistence\n"
            f"• Thumbnail support\n"
            f"• Auto-recovery\n"
            f"• RSS monitoring\n\n"
            f"📊 Status:\n"
            f"• Active downloads: {len(self.active_downloads)}\n"
            f"• Queue size: {self.download_queue.qsize()}"
        )

    async def stop(self):
        """Stop the bot"""
        logging.info("Stopping bot...")
        self.running = False
        if self.aria2_process:
            logging.info("Terminating aria2 process...")
            try:
                self.aria2_process.terminate()
                self.aria2_process.wait(timeout=5) # Wait for it to terminate
            except subprocess.TimeoutExpired:
                logging.warning("Aria2 process did not terminate gracefully, killing it.")
                self.aria2_process.kill()
            except Exception as e:
                logging.error(f"Error terminating aria2 process: {e}")
        await super().stop()
        logging.info("Bot stopped.")

    async def _feed_loop(self):
        """Main feed processing loop"""
        while self.running:
            try:
                logging.info("Starting new RSS feed processing cycle.")
                for url in PSA_FEEDS:
                    if not self.running:
                        break
                    await self.process_feed(url)
                    await asyncio.sleep(2)  # Small delay between feeds
                    
                # Wait 10 minutes before next cycle
                for _ in range(600):  # 600 seconds = 10 minutes
                    if not self.running:
                        break
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logging.error(f"Feed loop error: {e}")
                await asyncio.sleep(60) # Wait longer on error

# ------------------ Main ------------------
async def main():
    """Main async function"""
    # Start Flask in background
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Create and start bot
    bot = TorrentBot()
    
    try:
        await bot.start()
        logging.info("Bot started, waiting for events...")
        await idle()  # Keep the bot running until Ctrl+C
        
    except KeyboardInterrupt:
        logging.info("Bot stopped by user via KeyboardInterrupt")
    except Exception as e:
        logging.error(f"Bot crashed in main function: {e}", exc_info=True) # exc_info=True to print traceback
    finally:
        try:
            await bot.stop()
        except Exception as e:
            logging.error(f"Error during bot stop: {e}")

if __name__ == "__main__":
    # Ensure timedelta is correctly available
    from datetime import timedelta 
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Program interrupted by user (main process)")
    except Exception as e:
        logging.error(f"Program crashed at top level: {e}", exc_info=True)
        sys.exit(1)
