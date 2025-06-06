import asyncio
import feedparser
import json
import logging
import os
import re
import subprocess
import sys
import time
import threading  # MISSING IMPORT - Added this
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import aiohttp
import aria2p
from flask import Flask
from PIL import Image
from pymongo import MongoClient
from pyrogram import Client, errors
from config import BOT, API, OWNER

# ------------------ Constants ------------------
KEEP_ALIVE_URL = "https://abundant-barbie-musammiln-db578527.koyeb.app/"  # Change this
PSA_FEEDS = ["https://bt4gprx.com/search?q=psa&page=rss"]
MAX_FILE_SIZE_MB = 1900  # Reduced for free tier
DOWNLOAD_STATUS_UPDATE_INTERVAL = 10  # Increased interval
TELEGRAM_FILENAME_LIMIT = 60
SUFFIX = " -@MNTGX.-"
THUMBNAIL_URL = "https://i.ibb.co/MDwd1f3D/6087047735061627461.jpg"
MONGODB_URI = "mongodb+srv://mntgx:mntgx@cluster0.pzcpq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# ------------------ Database Setup ------------------
class MongoDB:
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client.torrent_bot
        self.completed = self.db.completed
        self.queue = self.db.queue
        self.active = self.db.active

    async def add_to_queue(self, content: str, title: str):
        self.queue.insert_one({
            "content": content,
            "title": title,
            "added_at": datetime.utcnow()
        })

    async def move_to_active(self, gid: str, content: str, title: str):
        self.queue.delete_one({"content": content})
        self.active.insert_one({
            "gid": gid,
            "content": content,
            "title": title,
            "started_at": datetime.utcnow()
        })

    async def move_to_completed(self, gid: str):
        active = self.active.find_one({"gid": gid})
        if active:
            self.active.delete_one({"gid": gid})
            self.completed.insert_one({
                **active,
                "completed_at": datetime.utcnow()
            })

    async def get_queued_items(self):
        return list(self.queue.find())

    async def get_active_downloads(self):
        return list(self.active.find())

    async def is_completed(self, content: str):
        return bool(self.completed.find_one({"content": content}))

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

def run_flask():
    app.run(host='0.0.0.0', port=8000)

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
        
        # Fallback to entry link
        return entry.link, 'link'
        
    except Exception as e:
        logging.error(f"Failed to extract torrent link: {e}")
        return entry.link, 'link'

# ------------------ Aria2 Setup ------------------
def start_aria2():
    try:
        # Kill any existing aria2 processes
        try:
            subprocess.run(["pkill", "-f", "aria2c"], check=False)
            time.sleep(2)
        except:
            pass
            
        return subprocess.Popen([
            "aria2c",
            "--enable-rpc",
            "--rpc-listen-all",
            "--rpc-listen-port=6800",
            f"--dir={DOWNLOAD_DIR}",
            "--max-concurrent-downloads=1",
            "--bt-max-peers=30",
            "--seed-time=0",
            "--quiet",
            "--daemon"
        ])
    except Exception as e:
        logging.error(f"Failed to start aria2: {e}")
        return None

# ------------------ Helper Functions ------------------
def add_suffix(filename: str) -> str:
    base, ext = os.path.splitext(filename)
    new_base = f"{base}{SUFFIX}"
    if len(new_base) + len(ext) > TELEGRAM_FILENAME_LIMIT:
        max_base_len = TELEGRAM_FILENAME_LIMIT - len(ext) - len(SUFFIX)
        new_base = f"{base[:max_base_len].rstrip()}{SUFFIX}"
    return f"{new_base}{ext}"

async def download_thumbnail() -> Optional[Path]:
    thumb_path = THUMBNAIL_DIR / "thumbnail.jpg"
    if thumb_path.exists():
        return thumb_path
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(THUMBNAIL_URL) as resp:
                if resp.status == 200:
                    with open(thumb_path, 'wb') as f:
                        f.write(await resp.read())
                    return thumb_path
    except Exception as e:
        logging.error(f"Thumbnail download failed: {e}")
        return None

def create_default_thumbnail() -> Path:
    thumb_path = THUMBNAIL_DIR / "default.jpg"
    try:
        img = Image.new('RGB', (320, 240), color='black')
        img.save(thumb_path, "JPEG", quality=80)
        return thumb_path
    except Exception as e:
        logging.error(f"Failed to create default thumb: {e}")
        return None

async def get_thumbnail() -> Optional[Path]:
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

    async def _rename_with_suffix(self, file_path: Path) -> Path:
        try:
            new_name = add_suffix(file_path.name)
            new_path = file_path.with_name(new_name)
            file_path.rename(new_path)
            return new_path
        except Exception as e:
            logging.error(f"Renaming failed: {e}")
            return file_path

    async def _upload_file(self, download):
        try:
            largest_file = max(download.files, key=lambda f: f.length)
            file_path = Path(largest_file.path)
            
            if not file_path.exists():
                raise FileNotFoundError("Downloaded file missing")
                
            if file_path.stat().st_size > MAX_FILE_SIZE_MB * 1024 * 1024:
                raise ValueError("File too large")
                
            file_path = await self._rename_with_suffix(file_path)
            
            if not self.thumbnail_path:
                self.thumbnail_path = await get_thumbnail()
                
            caption = (
                f"**{file_path.stem}**\n\n"
                f"Size: {file_path.stat().st_size / (1024*1024):.2f}MB\n"
                "Powered by @MNTGX"
            )
            
            await self.send_document(
                chat_id=self.owner_id,
                document=str(file_path),
                caption=caption,
                thumb=str(self.thumbnail_path) if self.thumbnail_path else None
            )
            
            file_path.unlink()
            await self.db.move_to_completed(download.gid)
            return True
            
        except Exception as e:
            logging.error(f"Upload failed: {e}")
            await self.send_message(
                self.owner_id,
                f"❌ Failed to upload {download.name}: {e}"
            )
            return False

    async def _process_queue(self):
        """Process items from database queue"""
        try:
            queued = await self.db.get_queued_items()
            for item in queued:
                await self.download_queue.put((item['content'], item['title']))
        except Exception as e:
            logging.error(f"Error processing queue: {e}")

    async def _recover_active(self):
        """Recover active downloads on restart"""
        try:
            active = await self.db.get_active_downloads()
            for item in active:
                try:
                    download = self.aria2.get_download(item['gid'])
                    if download.is_complete:
                        await self._upload_file(download)
                    elif not download.is_removed:
                        self.active_downloads[download.gid] = {
                            'title': item['title'],
                            'start_time': item['started_at'].timestamp()
                        }
                except Exception as e:
                    logging.error(f"Error recovering download {item['gid']}: {e}")
        except Exception as e:
            logging.error(f"Error recovering active downloads: {e}")

    async def _download_worker(self):
        while True:
            try:
                content, title = await self.download_queue.get()
                
                if await self.db.is_completed(content):
                    self.download_queue.task_done()
                    continue
                    
                if content.startswith('magnet:'):
                    download = self.aria2.add_magnet(content, {"dir": str(DOWNLOAD_DIR)})
                else:
                    download = self.aria2.add_torrent(content, {"dir": str(DOWNLOAD_DIR)})
                
                await self.db.move_to_active(download.gid, content, title)
                self.active_downloads[download.gid] = {
                    'title': title,
                    'start_time': time.time()
                }
                
                await self.send_message(
                    self.owner_id,
                    f"📥 Added to queue: {title}\n"
                    f"GID: {download.gid}"
                )
                
            except Exception as e:
                logging.error(f"Download failed: {title} - {e}")
                try:
                    await self.send_message(
                        self.owner_id,
                        f"❌ Download failed: {title}\nError: {e}"
                    )
                except:
                    pass
            finally:
                self.download_queue.task_done()

    async def _monitor_downloads(self):
        while True:
            try:
                await asyncio.sleep(DOWNLOAD_STATUS_UPDATE_INTERVAL)
                for gid, data in list(self.active_downloads.items()):
                    try:
                        download = self.aria2.get_download(gid)
                        
                        if download.is_complete:
                            if await self._upload_file(download):
                                await self.send_message(
                                    self.owner_id,
                                    f"✅ Completed: {data['title']}"
                                )
                            del self.active_downloads[gid]
                            
                        elif download.error_message:
                            await self.send_message(
                                self.owner_id,
                                f"❌ Failed: {data['title']}\nError: {download.error_message}"
                            )
                            del self.active_downloads[gid]
                            
                    except Exception as e:
                        logging.error(f"Monitor error for {gid}: {e}")
            except Exception as e:
                logging.error(f"Monitor loop error: {e}")
                await asyncio.sleep(30)

    async def process_feed(self, url):
        try:
            feed = fetch_feed(url)
            for entry in feed.entries[:3]:  # Only process 3 newest
                if await self.db.is_completed(entry.link):
                    continue
                    
                content, _ = download_torrent_or_link(entry)
                await self.db.add_to_queue(content, entry.title)
                await self.download_queue.put((content, entry.title))
                await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error processing feed {url}: {e}")

    async def start(self):
        await super().start()
        
        # Wait for aria2 to be ready
        await asyncio.sleep(5)
        
        try:
            self.aria2 = aria2p.API(aria2p.Client(host="http://localhost", port=6800, secret=""))
        except Exception as e:
            logging.error(f"Failed to connect to aria2: {e}")
            await self.send_message(self.owner_id, f"❌ Failed to connect to aria2: {e}")
            return
            
        self.thumbnail_path = await get_thumbnail()
        
        # Recover state
        await self._process_queue()
        await self._recover_active()
        
        # Start background tasks
        asyncio.create_task(self._download_worker())
        asyncio.create_task(self._monitor_downloads())
        asyncio.create_task(self._feed_loop())
        
        await self.send_message(self.owner_id, "✅ Bot started with:\n"
                                "- MongoDB persistence\n"
                                "- Thumbnail support\n"
                                "- Automatic recovery")

    async def _feed_loop(self):
        while True:
            try:
                for url in PSA_FEEDS:
                    await self.process_feed(url)
                await asyncio.sleep(600)  # Check every 10 minutes
            except Exception as e:
                logging.error(f"Feed loop error: {e}")
                await asyncio.sleep(60)

# ------------------ Main ------------------
if __name__ == "__main__":
    # Start aria2
    aria_process = start_aria2()
    if not aria_process:
        logging.error("Failed to start aria2")
        sys.exit(1)
    
    # Give aria2 time to start
    time.sleep(3)
    
    # Start Flask in background
    threading.Thread(target=run_flask, daemon=True).start()
    
    # Run bot
    bot = TorrentBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Bot crashed: {e}")
    finally:
        if aria_process:
            aria_process.terminate()
            aria_process.wait()
