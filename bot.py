import asyncio
import feedparser
import logging
import threading
import requests
import io
import re
import time
import subprocess
import os
import aiohttp
from datetime import datetime
from pathlib import Path
from flask import Flask
from PIL import Image
from pyrogram import Client, errors, filters
from pyrogram.types import Message
from config import BOT, API, OWNER, CHANNEL

# ------------------ Constants ------------------
KEEP_ALIVE_URL = "https://willowy-donny-kushpu-0e1a566f.koyeb.app/"
PSA_FEEDS = ["https://bt4gprx.com/search?q=psa&page=rss"]
MAX_FILE_SIZE_MB = 1900
DOWNLOAD_STATUS_UPDATE_INTERVAL = 10
TELEGRAM_FILENAME_LIMIT = 60
SUFFIX = " -@MNTGX.-"
THUMBNAIL_URL = "https://i.ibb.co/MDwd1f3D/6087047735061627461.jpg"
ARIA2_LOG_PATH = "aria2_debug.log"
ARIA2_RPC_PORT = 6800

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
    return "Bot is running!"

def run_flask():
    app.run(host='0.0.0.0', port=8000, debug=False, use_reloader=False)

# ------------------ Utility Functions ------------------
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
        if hasattr(entry, 'link') and entry.link.startswith('magnet:'):
            return entry.link, 'magnet'
        if hasattr(entry, 'enclosures'):
            for enclosure in entry.enclosures:
                if enclosure.type == 'application/x-bittorrent':
                    return enclosure.href, 'torrent'
        if hasattr(entry, 'links'):
            for link in entry.links:
                if link.type == 'application/x-bittorrent':
                    return link.href, 'torrent'
                elif 'magnet:' in link.href:
                    return link.href, 'magnet'
        if hasattr(entry, 'description'):
            magnet_match = re.search(r'magnet:\?[^\s<>"]*', entry.description)
            if magnet_match:
                return magnet_match.group(0), 'magnet'
        return entry.link, 'link'
    except Exception as e:
        logging.error(f"Failed to extract torrent link: {e}")
        return entry.link, 'link'

def is_valid_entry(entry):
    """Check if entry contains invalid keywords"""
    try:
        title = entry.title.lower()
        return not any(keyword in title for keyword in [
            "predvd", "prehd", "hdts", "camrip", "telesync", "screener", "ts", "dvdscr"
        ])
    except Exception as e:
        logging.error(f"Error checking entry validity: {e}")
        return False

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
        try:
            from PIL import ImageDraw, ImageFont
            draw = ImageDraw.Draw(img)
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

def add_suffix_to_filename(filename: str) -> str:
    """Add suffix to filename while respecting Telegram limits"""
    base, ext = os.path.splitext(filename)
    new_base = f"{base}{SUFFIX}"
    if len(new_base) + len(ext) > TELEGRAM_FILENAME_LIMIT:
        max_base_len = TELEGRAM_FILENAME_LIMIT - len(ext) - len(SUFFIX)
        new_base = f"{base[:max_base_len].rstrip()}{SUFFIX}"
    return f"{new_base}{ext}"

async def keep_alive():
    """Keep the bot alive by pinging the Koyeb URL"""
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(KEEP_ALIVE_URL) as resp:
                    logging.debug(f"Keep-alive ping: {resp.status}")
        except Exception as e:
            logging.warning(f"Keep-alive failed: {e}")
        await asyncio.sleep(300)  # Ping every 5 minutes

# ------------------ Aria2 Management ------------------
async def start_aria2():
    """Start aria2 daemon with retries"""
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            # Kill existing aria2 processes
            try:
                subprocess.run(["pkill", "-f", "aria2c"], timeout=5, check=False)
                await asyncio.sleep(2)
            except Exception as e:
                logging.warning(f"Error killing existing aria2: {e}")

            # Start new process
            cmd = [
                "aria2c",
                "--enable-rpc",
                "--rpc-listen-all",
                f"--rpc-listen-port={ARIA2_RPC_PORT}",
                "--rpc-allow-origin-all",
                f"--dir={DOWNLOAD_DIR}",
                "--max-concurrent-downloads=2",
                "--max-connection-per-server=8",
                "--min-split-size=1M",
                "--split=8",
                "--bt-max-peers=50",
                "--seed-time=0",
                "--daemon=true",
                f"--log={ARIA2_LOG_PATH}",
                "--log-level=info"
            ]
            
            logging.info(f"Starting aria2 (attempt {attempt}/{max_retries}): {' '.join(cmd)}")
            process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            await asyncio.sleep(5)  # Give it time to start

            # Verify connection
            try:
                import aria2p
                client = aria2p.Client(
                    host="http://localhost",
                    port=ARIA2_RPC_PORT,
                    timeout=10
                )
                api = aria2p.API(client)
                version = api.get_version()
                logging.info(f"Aria2 started successfully. Version: {version.version}")
                return process, api
            except Exception as e:
                logging.error(f"Aria2 RPC connection failed: {e}")
                raise

        except Exception as e:
            logging.error(f"Aria2 startup attempt {attempt} failed: {e}")
            if attempt < max_retries:
                await asyncio.sleep(5)
            else:
                return None, None

# ------------------ Bot Class ------------------
class MN_Bot(Client):
    def __init__(self):
        super().__init__(
            "MN-Bot",
            api_id=API.ID,
            api_hash=API.HASH,
            bot_token=BOT.TOKEN,
            workers=8
        )
        self.channel_id = CHANNEL.ID
        self.owner_id = OWNER.ID
        self.posted = set()
        self.thumbnail_path = None
        self.active_downloads = {}  # {gid: download_info}
        self.download_queue = asyncio.Queue()
        self.aria2_process = None
        self.aria2 = None
        self.running = False

    async def ensure_aria2_connection(self):
        """Ensure we have a working aria2 connection"""
        if self.aria2:
            try:
                self.aria2.get_version()
                return True
            except Exception:
                pass
        
        logging.warning("Aria2 connection lost, attempting to reconnect...")
        self.aria2_process, self.aria2 = await start_aria2()
        
        if self.aria2:
            await self.send_message(
                self.owner_id,
                "♻️ Reconnected to Aria2 successfully"
            )
            return True
        else:
            await self.send_message(
                self.owner_id,
                "❌ Failed to reconnect to Aria2. Downloads will not work."
            )
            return False

    async def _add_torrent_to_session(self, content: str, title: str):
        """Add torrent/magnet to aria2"""
        if not await self.ensure_aria2_connection():
            return None

        try:
            if content.startswith('magnet:'):
                download = self.aria2.add_magnet(content, {"dir": str(DOWNLOAD_DIR)})
            else:
                download = self.aria2.add_uri([content], {"dir": str(DOWNLOAD_DIR)})
            
            # Create status message
            msg = await self.send_message(
                self.owner_id,
                f"📥 **Download Started**\n"
                f"📁 {title[:50]}{'...' if len(title) > 50 else ''}\n"
                f"🆔 GID: `{download.gid}`"
            )
            
            self.active_downloads[download.gid] = {
                'title': title,
                'content': content,
                'message': msg,
                'start_time': time.time(),
                'last_update': time.time()
            }
            
            logging.info(f"Started download: {title} (GID: {download.gid})")
            return download.gid
            
        except Exception as e:
            logging.error(f"Download start failed for '{title}': {e}")
            await self.send_message(
                self.owner_id,
                f"❌ **Download Failed to Start**\n"
                f"📁 {title[:50]}{'...' if len(title) > 50 else ''}\n"
                f"🚫 Error: {str(e)[:100]}"
            )
            return None

    async def _update_download_progress(self):
        """Monitor active downloads"""
        while self.running:
            try:
                if not await self.ensure_aria2_connection():
                    await asyncio.sleep(30)
                    continue

                await asyncio.sleep(DOWNLOAD_STATUS_UPDATE_INTERVAL)
                
                for gid in list(self.active_downloads.keys()):
                    if not self.running:
                        break
                        
                    try:
                        download = self.aria2.get_download(gid)
                        data = self.active_downloads[gid]
                        
                        if download.is_complete:
                            if await self._upload_file(download):
                                await data['message'].edit_text(
                                    f"✅ **Download Complete**\n"
                                    f"📁 {data['title'][:50]}{'...' if len(data['title']) > 50 else ''}"
                                )
                            del self.active_downloads[gid]
                            
                        elif download.status == "error":
                            error_msg = download.error_message or "Unknown error"
                            await data['message'].edit_text(
                                f"❌ **Download Failed**\n"
                                f"📁 {data['title'][:50]}{'...' if len(data['title']) > 50 else ''}\n"
                                f"🚫 Error: {error_msg[:100]}"
                            )
                            del self.active_downloads[gid]
                            
                        elif download.is_active:
                            # Update progress
                            progress = download.progress
                            speed = download.download_speed / 1024  # KB/s
                            downloaded = download.completed_length / (1024 * 1024)  # MB
                            total = download.total_length / (1024 * 1024)  # MB
                            
                            if time.time() - data['last_update'] > DOWNLOAD_STATUS_UPDATE_INTERVAL:
                                await data['message'].edit_text(
                                    f"⏳ **Downloading** ({progress:.1f}%)\n"
                                    f"📁 {data['title'][:50]}{'...' if len(data['title']) > 50 else ''}\n"
                                    f"📊 {downloaded:.1f}MB / {total:.1f}MB\n"
                                    f"⚡ {speed:.1f} KB/s"
                                )
                                data['last_update'] = time.time()
                                
                    except aria2p.client.ClientException as e:
                        if "not found" in str(e).lower():
                            logging.info(f"Download {gid} not found, cleaning up")
                            del self.active_downloads[gid]
                        else:
                            logging.error(f"Aria2 client error for {gid}: {e}")
                            self.aria2 = None  # Force reconnect
                            break
                            
                    except Exception as e:
                        logging.error(f"Error monitoring {gid}: {e}")
                        continue
                        
            except Exception as e:
                logging.error(f"Monitor loop error: {e}")
                await asyncio.sleep(30)

    async def _upload_file(self, download):
        """Upload completed file to Telegram"""
        try:
            # Find the largest downloaded file
            largest_file = None
            for file in download.files:
                file_path = Path(file.path)
                if file_path.exists() and file_path.stat().st_size > 0:
                    if not largest_file or file.length > largest_file.length:
                        largest_file = file
            
            if not largest_file:
                logging.error(f"No valid files found for {download.name}")
                return False
                
            file_path = Path(largest_file.path)
            if file_path.stat().st_size > MAX_FILE_SIZE_MB * 1024 * 1024:
                logging.error(f"File too large: {file_path}")
                return False
                
            # Rename file
            new_path = file_path.with_name(add_suffix_to_filename(file_path.name))
            file_path.rename(new_path)
            
            # Get thumbnail
            if not self.thumbnail_path:
                self.thumbnail_path = await download_thumbnail() or create_default_thumbnail()
            
            # Upload
            await self.send_document(
                chat_id=self.channel_id,
                document=str(new_path),
                caption=f"**{download.name}**\n\n⚡ Powered by @MNTGX",
                thumb=str(self.thumbnail_path) if self.thumbnail_path else None
            )
            
            # Cleanup
            new_path.unlink()
            return True
            
        except Exception as e:
            logging.error(f"Upload failed for {download.name}: {e}")
            return False

    async def _download_worker(self):
        """Process download queue"""
        while self.running:
            try:
                content, title = await asyncio.wait_for(
                    self.download_queue.get(), 
                    timeout=5.0
                )
                
                await self._add_torrent_to_session(content, title)
                self.download_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logging.error(f"Download worker error: {e}")
                await asyncio.sleep(5)

    async def process_psa_feed(self, url):
        """Process RSS feed"""
        feed = fetch_feed(url)
        for entry in feed.entries[:5]:  # Process only latest 5 entries
            if entry.link in self.posted or not is_valid_entry(entry):
                continue
                
            content, kind = download_torrent_or_link(entry)
            if kind in ('magnet', 'torrent'):
                await self.download_queue.put((content, entry.title))
                self.posted.add(entry.link)
                await asyncio.sleep(1)

    async def initial_post(self):
        """Post initial items from feed"""
        for url in PSA_FEEDS:
            await self.process_psa_feed(url)
            await asyncio.sleep(2)

    async def auto_post(self):
        """Periodically check feeds"""
        while self.running:
            try:
                for url in PSA_FEEDS:
                    await self.process_psa_feed(url)
                    await asyncio.sleep(5)
                await asyncio.sleep(300)  # 5 minutes between checks
            except Exception as e:
                logging.error(f"Auto-post error: {e}")
                await asyncio.sleep(60)

    async def start(self):
        """Start the bot"""
        await super().start()
        self.running = True
        
        # Start aria2
        self.aria2_process, self.aria2 = await start_aria2()
        if not self.aria2:
            await self.send_message(self.owner_id, "❌ Failed to start Aria2. Downloads will not work.")
        
        # Initialize thumbnail
        self.thumbnail_path = await download_thumbnail() or create_default_thumbnail()
        
        # Start background tasks
        asyncio.create_task(keep_alive())
        asyncio.create_task(self._update_download_progress())
        asyncio.create_task(self._download_worker())
        asyncio.create_task(self.auto_post())
        
        # Initial setup
        await self.initial_post()
        me = await self.get_me()
        await self.send_message(
            self.owner_id,
            f"✅ **Bot Started**\n\n"
            f"• Username: @{me.username}\n"
            f"• Aria2: {'✅' if self.aria2 else '❌'}\n"
            f"• Queue: {self.download_queue.qsize()} items"
        )

    async def stop(self, *args):
        """Stop the bot"""
        logging.info("Stopping bot...")
        self.running = False
        
        # Stop aria2
        if self.aria2_process:
            try:
                self.aria2_process.terminate()
                await asyncio.sleep(2)
                if self.aria2_process.poll() is None:
                    self.aria2_process.kill()
            except Exception as e:
                logging.error(f"Error stopping aria2: {e}")
        
        await super().stop()
        logging.info("Bot stopped")

# ------------------ Main ------------------
async def main():
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    bot = MN_Bot()
    try:
        await bot.start()
        await idle()
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.critical(f"Bot crashed: {e}")
    finally:
        await bot.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.critical(f"Fatal error: {e}")
