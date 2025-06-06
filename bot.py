import asyncio
import feedparser
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import aiohttp
import aria2p
from flask import Flask
from pyrogram import Client, errors
from pyrogram.types import Message
from config import BOT, API, OWNER

# ------------------ Constants ------------------
KEEP_ALIVE_URL = "https://willowy-donny-kushpu-0e1a566f.koyeb.app/"
PSA_FEEDS = ["https://bt4gprx.com/search?q=psa&page=rss"]
MAX_FILE_SIZE_MB = 1900  # 1.9GB
MAX_TELEGRAM_SIZE = 2000  # 2GB
DOWNLOAD_STATUS_UPDATE_INTERVAL = 5  # seconds

FILTER_KEYWORDS = [
    "predvd", "prehd", "hdts", "camrip", "telesync", 
    "screener", "ts", "dvdscr", "hdcam", "hdrip"
]

# ------------------ Path Setup ------------------
BASE_DIR = Path(__file__).parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
THUMBNAIL_DIR = BASE_DIR / "thumbnails"
CONFIG_DIR = BASE_DIR / "config"

DOWNLOAD_DIR.mkdir(exist_ok=True)
THUMBNAIL_DIR.mkdir(exist_ok=True)
CONFIG_DIR.mkdir(exist_ok=True)

# ------------------ Logging Setup ------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(BASE_DIR / "bot.log"),
        logging.StreamHandler()
    ]
)
logging.getLogger("pyrogram").setLevel(logging.ERROR)

# ------------------ Flask App for Health Check ------------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Torrent Bot is running!"

def run_flask():
    app.run(host='0.0.0.0', port=8000)

# ------------------ Helper Functions ------------------
def load_config() -> Dict:
    config_file = CONFIG_DIR / "config.json"
    if config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    return {"posted": []}

def save_config(config: Dict):
    config_file = CONFIG_DIR / "config.json"
    with open(config_file, 'w') as f:
        json.dump(config, f)

def download_torrent_or_link(entry) -> Tuple[str, str]:
    if entry.enclosures:
        return entry.enclosures[0].href, 'torrent'
    link = entry.link
    if link.lower().endswith('.torrent'):
        return link, 'torrent'
    return link, 'link'

def is_valid_entry(entry) -> bool:
    desc = entry.get("description", "").lower()
    title = entry.title.lower()
    
    # Size check
    size_match = re.search(r"(\d+(?:\.\d+)?)\s*(GB|MB|KB)", desc, re.IGNORECASE)
    if size_match:
        size_value = float(size_match.group(1))
        size_unit = size_match.group(2).upper()
        
        size_mb = {
            "GB": size_value * 1024,
            "MB": size_value,
            "KB": size_value / 1024
        }.get(size_unit, 0)
        
        if size_mb > MAX_FILE_SIZE_MB:
            logging.info(f"Skipping '{entry.title}' due to size: {size_value}{size_unit}")
            return False
    
    # Keyword check
    if any(keyword in title for keyword in FILTER_KEYWORDS):
        logging.info(f"Skipping '{entry.title}' due to forbidden keyword")
        return False
        
    return True

async def download_thumbnail() -> Optional[Path]:
    """Download and return thumbnail path if successful"""
    thumbnail_url = "https://i.ibb.co/MDwd1f3D/6087047735061627461.jpg"
    thumbnail_path = THUMBNAIL_DIR / "custom_thumb.jpg"
    
    if thumbnail_path.exists():
        return thumbnail_path
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(thumbnail_url) as response:
                if response.status == 200:
                    content = await response.read()
                    with open(thumbnail_path, 'wb') as f:
                        f.write(content)
                    return thumbnail_path
    except Exception as e:
        logging.error(f"Error downloading thumbnail: {e}")
        return None

def create_default_thumbnail() -> Path:
    """Create a default black thumbnail"""
    thumb_path = THUMBNAIL_DIR / "default_thumb.jpg"
    try:
        img = Image.new('RGB', (320, 240), color='black')
        img.save(thumb_path, "JPEG", quality=85)
        return thumb_path
    except Exception as e:
        logging.error(f"Error creating default thumbnail: {e}")
        return None

async def keep_alive():
    """Periodically ping keep-alive URL"""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await session.get(KEEP_ALIVE_URL)
                logging.debug("Sent keep-alive request")
            except Exception as e:
                logging.error(f"Keep-alive failed: {e}")
            await asyncio.sleep(111)

# ------------------ Bot Class ------------------
class TorrentBot(Client):
    def __init__(self):
        super().__init__(
            "TorrentBot",
            api_id=API.ID,
            api_hash=API.HASH,
            bot_token=BOT.TOKEN,
            workers=16,
        )
        self.owner_id = OWNER.ID
        self.config = load_config()
        self.thumbnail_path = None
        
        # Initialize aria2
        self.aria2 = aria2p.API(
            aria2p.Client(
                host="http://localhost",
                port=6800,
                secret=""
            )
        )
        
        self.active_downloads = {}  # {gid: {message, entry_title, etc}}
        self.download_queue = asyncio.Queue()
        self.lock = asyncio.Lock()

    async def _monitor_downloads(self):
        """Monitor and update progress of active downloads"""
        while True:
            await asyncio.sleep(DOWNLOAD_STATUS_UPDATE_INTERVAL)
            
            async with self.lock:
                for gid, data in list(self.active_downloads.items()):
                    try:
                        download = self.aria2.get_download(gid)
                        
                        if download.is_complete:
                            await self._handle_completed_download(download, data)
                            continue
                            
                        if download.error_message:
                            await self._handle_failed_download(download, data)
                            continue
                            
                        await self._update_progress_message(download, data)
                    except Exception as e:
                        logging.error(f"Error monitoring download {gid}: {e}")

    async def _handle_completed_download(self, download, data):
        """Handle completed download"""
        entry_title = data['entry_title']
        
        logging.info(f"Completed: {entry_title}")
        await data['message'].edit_text(
            f"✅ **Download Completed:** `{entry_title}`\n"
            f"**Size:** `{download.total_length / (1024**2):.2f} MB`"
        )
        
        await self._send_to_owner(download)
        
        async with self.lock:
            del self.active_downloads[download.gid]
            download.remove(force=True)

    async def _handle_failed_download(self, download, data):
        """Handle failed download"""
        entry_title = data['entry_title']
        
        logging.error(f"Failed: {entry_title} - {download.error_message}")
        await data['message'].edit_text(
            f"❌ **Download Failed:** `{entry_title}`\n"
            f"**Error:** `{download.error_message}`"
        )
        
        async with self.lock:
            del self.active_downloads[download.gid]
            download.remove(force=True)

    async def _update_progress_message(self, download, data):
        """Update progress message"""
        progress = download.progress * 100
        speed = download.download_speed / 1024  # KB/s
        
        try:
            await data['message'].edit_text(
                f"**Downloading:** `{data['entry_title']}`\n"
                f"**Progress:** `{progress:.2f}%` {self._progress_bar(progress)}\n"
                f"**Downloaded:** `{download.completed_length / (1024**2):.2f} MB`\n"
                f"**Speed:** `{speed:.2f} KB/s`"
            )
        except errors.MessageNotModified:
            pass
        except Exception as e:
            logging.error(f"Error updating message: {e}")

    def _progress_bar(self, progress: float, length: int = 20) -> str:
        """Create progress bar string"""
        filled = int(length * progress // 100)
        return f"[{'█' * filled}{' ' * (length - filled)}]"

    async def _send_to_owner(self, download):
        """Send downloaded files to owner"""
        try:
            files = download.files
            if not files:
                raise ValueError("No files in download")
                
            # Get the first file (modify as needed for multi-file torrents)
            file_path = Path(files[0].path)
            
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
                
            file_size = file_path.stat().st_size / (1024**2)  # MB
            
            if file_size > MAX_TELEGRAM_SIZE:
                raise ValueError(f"File too large for Telegram ({file_size:.2f}MB)")
                
            if not self.thumbnail_path:
                self.thumbnail_path = await download_thumbnail() or create_default_thumbnail()
                
            await self.send_document(
                chat_id=self.owner_id,
                document=str(file_path),
                caption=f"**{download.name}**\n\nDownload completed by TorrentBot",
                thumb=str(self.thumbnail_path) if self.thumbnail_path else None
            )
            
            file_path.unlink()
            logging.info(f"Sent and removed: {file_path}")
            
        except Exception as e:
            logging.error(f"Error sending file: {e}")
            await self.send_message(
                self.owner_id,
                f"❌ **Failed to send file:** `{download.name}`\nError: `{e}`"
            )

    async def _download_worker(self):
        """Process download queue items one at a time"""
        while True:
            content, entry_title = await self.download_queue.get()
            
            try:
                # Add download to aria2
                if content.startswith('magnet:'):
                    download = self.aria2.add_magnet(content, {"dir": str(DOWNLOAD_DIR)})
                else:
                    download = self.aria2.add_torrent(content, {"dir": str(DOWNLOAD_DIR)})
                
                msg = await self.send_message(
                    self.owner_id,
                    f"📥 **Starting Download:** `{entry_title}`\n"
                    f"**Progress:** `0.00%` [                    ]\n"
                    f"**Speed:** `0.00 KB/s`"
                )
                
                async with self.lock:
                    self.active_downloads[download.gid] = {
                        'message': msg,
                        'entry_title': entry_title
                    }
                    
            except Exception as e:
                logging.error(f"Download worker error: {e}")
                await self.send_message(
                    self.owner_id,
                    f"❌ **Download Failed:** `{entry_title}`\nError: `{e}`"
                )
            finally:
                if not content.startswith('magnet:') and os.path.exists(content):
                    os.remove(content)
                    
                self.download_queue.task_done()

    async def process_feed(self, url: str):
        """Process RSS feed and queue valid entries"""
        feed = fetch_feed(url)
        for entry in feed.entries:
            if entry.link in self.config['posted'] or not is_valid_entry(entry):
                continue
                
            content, kind = download_torrent_or_link(entry)
            await self.download_queue.put((content, entry.title))
            self.config['posted'].append(entry.link)
            save_config(self.config)
            await asyncio.sleep(1)

    async def auto_feed_check(self):
        """Periodically check feeds"""
        while True:
            try:
                for url in PSA_FEEDS:
                    await self.process_feed(url)
                    await asyncio.sleep(5)
                await asyncio.sleep(300)  # 5 minutes between checks
            except Exception as e:
                logging.error(f"Feed check error: {e}")
                await asyncio.sleep(60)

    async def start(self):
        await super().start()
        
        # Initialize
        me = await self.get_me()
        self.thumbnail_path = await download_thumbnail() or create_default_thumbnail()
        
        # Start background tasks
        asyncio.create_task(keep_alive())
        asyncio.create_task(self._monitor_downloads())
        asyncio.create_task(self._download_worker())
        asyncio.create_task(self.auto_feed_check())
        
        await self.send_message(
            self.owner_id,
            f"✅ **TorrentBot Started**\n"
            f"Username: @{me.username}\n"
            f"Monitoring {len(PSA_FEEDS)} feeds"
        )

    async def stop(self, *args):
        logging.info("Stopping bot...")
        for download in self.aria2.get_downloads():
            download.remove(force=True)
        await super().stop()

# ------------------ Main ------------------
if __name__ == "__main__":
    # Start aria2c process
    aria2_process = subprocess.Popen([
        "aria2c",
        "--enable-rpc",
        "--rpc-listen-all=true",
        "--rpc-allow-origin-all",
        "--dir=/app/downloads",
        "--max-concurrent-downloads=1",
        "--seed-time=0",
        "--bt-max-peers=50",
        "--daemon=true"
    ])
    
    # Start Flask in a thread
    threading.Thread(target=run_flask, daemon=True).start()
    
    # Run the bot
    bot = TorrentBot()
    bot.run()
