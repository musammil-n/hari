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
from datetime import datetime
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
    try:
        # Kill any existing aria2 processes
        try:
            subprocess.run(["pkill", "-f", "aria2c"], check=False, timeout=5)
            await asyncio.sleep(2)
        except:
            pass
        
        # Start aria2 daemon
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
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Wait for aria2 to start
        await asyncio.sleep(5)
        
        # Test connection
        for i in range(10):
            try:
                api = aria2p.API(aria2p.Client(host="http://localhost", port=6800, secret=""))
                api.get_version()
                logging.info("Aria2 started successfully")
                return process, api
            except:
                await asyncio.sleep(2)
        
        raise Exception("Failed to connect to aria2 after 10 attempts")
        
    except Exception as e:
        logging.error(f"Failed to start aria2: {e}")
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
            draw.text((50, 100), "MNTGX", fill=(255, 255, 255))
        except:
            pass
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
                    return file_path
            
            return None
            
        except Exception as e:
            logging.error(f"Error getting largest file: {e}")
            return None

    async def _upload_file(self, download):
        """Upload completed file to Telegram"""
        try:
            file_path = await self._get_largest_file(download)
            if not file_path:
                raise FileNotFoundError("No valid downloaded file found")
            
            file_size = file_path.stat().st_size
            if file_size > MAX_FILE_SIZE_MB * 1024 * 1024:
                raise ValueError(f"File too large: {file_size / (1024*1024):.2f}MB")
            
            if file_size < 1024:  # Less than 1KB
                raise ValueError("File too small, possibly corrupted")
            
            file_path = await self._rename_with_suffix(file_path)
            
            if not self.thumbnail_path:
                self.thumbnail_path = await get_thumbnail()
            
            caption = (
                f"**{file_path.stem.replace(SUFFIX, '')}**\n\n"
                f"📁 Size: {file_size / (1024*1024):.2f} MB\n"
                f"🔗 Source: RSS Feed\n"
                f"⚡ Powered by @MNTGX"
            )
            
            await self.send_document(
                chat_id=self.owner_id,
                document=str(file_path),
                caption=caption,
                thumb=str(self.thumbnail_path) if self.thumbnail_path else None,
                progress=self._upload_progress
            )
            
            # Clean up file after successful upload
            try:
                file_path.unlink()
                # Also clean up parent directory if empty
                if file_path.parent != DOWNLOAD_DIR and not any(file_path.parent.iterdir()):
                    file_path.parent.rmdir()
            except:
                pass
            
            self.db.move_to_completed(download.gid)
            return True
            
        except Exception as e:
            logging.error(f"Upload failed: {e}")
            await self.send_message(
                self.owner_id,
                f"❌ **Upload Failed**\n"
                f"📁 File: {getattr(download, 'name', 'Unknown')}\n"
                f"🚫 Error: {str(e)[:100]}"
            )
            return False

    async def _upload_progress(self, current, total):
        """Upload progress callback"""
        try:
            percent = current * 100 / total
            if percent % 20 == 0:  # Log every 20%
                logging.info(f"Upload progress: {percent:.1f}%")
        except:
            pass

    async def _process_queue(self):
        """Process items from database queue"""
        try:
            queued = self.db.get_queued_items()
            logging.info(f"Processing {len(queued)} queued items")
            for item in queued:
                await self.download_queue.put((item['content'], item['title']))
        except Exception as e:
            logging.error(f"Error processing queue: {e}")

    async def _recover_active(self):
        """Recover active downloads on restart"""
        try:
            # Clean up stale downloads first
            self.db.cleanup_stale_active()
            
            active = self.db.get_active_downloads()
            logging.info(f"Recovering {len(active)} active downloads")
            
            for item in active:
                try:
                    download = self.aria2.get_download(item['gid'])
                    if download.is_complete:
                        logging.info(f"Found completed download: {item['title']}")
                        await self._upload_file(download)
                    elif download.is_active:
                        self.active_downloads[download.gid] = {
                            'title': item['title'],
                            'start_time': item['started_at'].timestamp() if 'started_at' in item else time.time()
                        }
                        logging.info(f"Recovered active download: {item['title']}")
                    else:
                        # Remove stale database entry
                        self.db.active.delete_one({"gid": item['gid']})
                        
                except aria2p.ClientException as e:
                    if "not found" in str(e).lower():
                        # GID not found, remove from database
                        self.db.active.delete_one({"gid": item['gid']})
                        logging.info(f"Removed stale GID: {item['gid']}")
                    else:
                        logging.error(f"Error recovering download {item['gid']}: {e}")
                except Exception as e:
                    logging.error(f"Error recovering download {item['gid']}: {e}")
                    
        except Exception as e:
            logging.error(f"Error recovering active downloads: {e}")

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
                    continue
                
                # Check if already completed
                if self.db.is_completed(content):
                    logging.info(f"Skipping already completed: {title}")
                    self.download_queue.task_done()
                    continue
                
                # Add download to aria2
                try:
                    if content.startswith('magnet:'):
                        download = self.aria2.add_magnet(content, {"dir": str(DOWNLOAD_DIR)})
                    else:
                        download = self.aria2.add_torrent(content, {"dir": str(DOWNLOAD_DIR)})
                    
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
                    
                    logging.info(f"Started download: {title}")
                    
                except Exception as e:
                    logging.error(f"Download start failed: {title} - {e}")
                    await self.send_message(
                        self.owner_id,
                        f"❌ **Download Failed to Start**\n"
                        f"📁 {title[:50]}{'...' if len(title) > 50 else ''}\n"
                        f"🚫 Error: {str(e)[:100]}"
                    )
                finally:
                    self.download_queue.task_done()
                    
            except Exception as e:
                logging.error(f"Download worker error: {e}")
                await asyncio.sleep(5)

    async def _monitor_downloads(self):
        """Monitor active downloads"""
        while self.running:
            try:
                await asyncio.sleep(DOWNLOAD_STATUS_UPDATE_INTERVAL)
                
                for gid, data in list(self.active_downloads.items()):
                    try:
                        download = self.aria2.get_download(gid)
                        
                        if download.is_complete:
                            logging.info(f"Download completed: {data['title']}")
                            if await self._upload_file(download):
                                await self.send_message(
                                    self.owner_id,
                                    f"✅ **Upload Complete**\n"
                                    f"📁 {data['title'][:50]}{'...' if len(data['title']) > 50 else ''}"
                                )
                            del self.active_downloads[gid]
                            
                        elif download.status == "error":
                            error_msg = download.error_message or "Unknown error"
                            logging.error(f"Download failed: {data['title']} - {error_msg}")
                            await self.send_message(
                                self.owner_id,
                                f"❌ **Download Failed**\n"
                                f"📁 {data['title'][:50]}{'...' if len(data['title']) > 50 else ''}\n"
                                f"🚫 Error: {error_msg[:100]}"
                            )
                            self.db.active.delete_one({"gid": gid})
                            del self.active_downloads[gid]
                            
                    except aria2p.ClientException as e:
                        if "not found" in str(e).lower():
                            # Download was removed, clean up
                            self.db.active.delete_one({"gid": gid})
                            if gid in self.active_downloads:
                                del self.active_downloads[gid]
                        else:
                            logging.error(f"Monitor error for {gid}: {e}")
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
                        continue
                    
                    content, link_type = download_torrent_or_link(entry)
                    if content and content != entry.link:  # Valid torrent/magnet found
                        self.db.add_to_queue(content, entry.title)
                        await self.download_queue.put((content, entry.title))
                        processed += 1
                        await asyncio.sleep(1)  # Rate limiting
                        
                except Exception as e:
                    logging.error(f"Error processing entry {entry.title}: {e}")
                    
            if processed > 0:
                logging.info(f"Added {processed} new downloads from feed")
                
        except Exception as e:
            logging.error(f"Error processing feed {url}: {e}")

    async def start(self):
        """Start the bot"""
        await super().start()
        self.running = True
        
        # Start aria2
        self.aria2_process, self.aria2 = await start_aria2()
        if not self.aria2:
            await self.send_message(self.owner_id, "❌ Failed to start aria2 daemon")
            return
            
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
        self.running = False
        if self.aria2_process:
            self.aria2_process.terminate()
        await super().stop()

    async def _feed_loop(self):
        """Main feed processing loop"""
        while self.running:
            try:
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
                await asyncio.sleep(60)

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
        await idle()  # Keep the bot running
        
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Bot crashed: {e}")
    finally:
        try:
            await bot.stop()
        except:
            pass

if __name__ == "__main__":
    # Fix for missing datetime import
    from datetime import timedelta
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Program interrupted by user")
    except Exception as e:
        logging.error(f"Program crashed: {e}")
        sys.exit(1)
