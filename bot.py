import asyncio
import json
import logging
import os
import subprocess
import sys
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from config import BOT, API, OWNER
import aiohttp
import aria2p
from flask import Flask
from PIL import Image
from pymongo import MongoClient
from pyrogram import Client, filters, idle, errors

# ------------------ Constants ------------------
KEEP_ALIVE_URL = "https://abundant-barbie-musammiln-db578527.koyeb.app/"  # Optional: For hosting platforms like Koyeb
MONGODB_URI = "mongodb+srv://mntgx:mntgx@cluster0.pzcpq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MAX_FILE_SIZE_MB = 1900  # Telegram limit (adjust as needed)
DOWNLOAD_STATUS_UPDATE_INTERVAL = 10
TELEGRAM_FILENAME_LIMIT = 64
SUFFIX = " -@VideoDLBot.-"
THUMBNAIL_URL = "https://i.ibb.co/MDwd1f3D/6087047735061627461.jpg"  # Replace with your thumbnail URL

# ------------------ Database Setup ------------------
class MongoDB:
    def __init__(self):
        try:
            self.client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            self.db = self.client.torrent_bot
            self.completed = self.db.completed
            self.queue = self.db.queue
            self.active = self.db.active
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
        try:
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
    return "Video Torrent Bot is running!"

@app.route('/health')
def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

def run_flask():
    try:
        app.run(host='0.0.0.0', port=8000, debug=False, use_reloader=False)
    except Exception as e:
        logging.error(f"Flask server error: {e}")

# ------------------ Aria2 Setup ------------------
async def start_aria2():
    process = None
    api = None
    try:
        if os.path.exists("/usr/bin/killall") or os.path.exists("/bin/killall"):
            subprocess.run(["killall", "-q", "aria2c"], check=False, timeout=5)
            await asyncio.sleep(2)
        else:
            logging.warning("killall not found, checking for aria2c processes...")
            try:
                output = subprocess.check_output(["ps", "aux"]).decode()
                for line in output.splitlines():
                    if "aria2c" in line and "grep" not in line:
                        pid = line.split()[1]
                        subprocess.run(["kill", "-9", pid], check=False, timeout=2)
                        await asyncio.sleep(1)
            except Exception as e:
                logging.warning(f"Could not kill aria2c processes: {e}")

        aria2_log_path = BASE_DIR / "aria2_debug.log"
        with open(aria2_log_path, "w") as log_file:
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
            ], stdout=subprocess.DEVNULL, stderr=log_file)

        logging.info(f"Aria2 process started with PID: {process.pid}")
        await asyncio.sleep(7)

        for i in range(15):
            try:
                api = aria2p.API(aria2p.Client(host="http://localhost", port=6800, secret=""))
                api.get_version()
                logging.info("Aria2 started successfully")
                return process, api
            except Exception as e:
                logging.warning(f"Attempt {i+1}/15 to connect to aria2 failed: {e}")
                if process.poll() is not None:
                    logging.error(f"Aria2 process exited with code {process.poll()}")
                    return None, None
                await asyncio.sleep(5)

        logging.error("Failed to connect to aria2 RPC after 15 attempts")
        return None, None
    except Exception as e:
        logging.error(f"Failed to start aria2: {e}")
        if process:
            process.terminate()
        return None, None

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
    thumb_path = THUMBNAIL_DIR / "default.jpg"
    try:
        img = Image.new('RGB', (320, 240), color=(25, 25, 25))
        try:
            from PIL import ImageDraw
            draw = ImageDraw.Draw(img)
            draw.text((50, 100), "VideoDLBot", fill=(255, 255, 255))
        except Exception as e:
            logging.warning(f"Failed to add text to thumbnail: {e}")
        img.save(thumb_path, "JPEG", quality=80)
        return thumb_path
    except Exception as e:
        logging.error(f"Failed to create default thumb: {e}")
        return None

async def get_thumbnail() -> Optional[Path]:
    thumb = await download_thumbnail()
    return thumb or create_default_thumbnail()

# ------------------ Bot Class ------------------
class VideoTorrentBot(Client):
    def __init__(self):
        super().__init__(
            "VideoTorrentBot",
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
        try:
            new_name = add_suffix(file_path.name)
            new_path = file_path.with_name(new_name)
            if file_path.exists():
                file_path.rename(new_path)
            return new_path
        except Exception as e:
            logging.error(f"Renaming failed for {file_path.name}: {e}")
            return file_path

    async def _get_video_file(self, download) -> Optional[Path]:
        try:
            if not download.files:
                logging.warning(f"No files found for download GID: {download.gid}")
                return None
            video_extensions = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv'}
            for file in download.files:
                file_path = Path(file.path)
                if file_path.suffix.lower() in video_extensions and file_path.exists() and file_path.stat().st_size > 1024:
                    return file_path
            logging.warning(f"No valid video file found for GID: {download.gid}")
            return None
        except Exception as e:
            logging.error(f"Error getting video file for {download.gid}: {e}")
            return None

    async def _upload_file(self, download):
        try:
            file_path = await self._get_video_file(download)
            if not file_path:
                logging.error(f"Upload failed for GID {download.gid}: No valid video file found")
                await self.send_message(
                    self.owner_id,
                    f"❌ **Upload Failed**\n"
                    f"📁 File: {getattr(download, 'name', 'Unknown')}\n"
                    f"🚫 Error: No valid video file found"
                )
                return False

            file_size = file_path.stat().st_size
            if file_size > MAX_FILE_SIZE_MB * 1024 * 1024:
                logging.error(f"Upload failed for GID {download.gid}: File too large ({file_size / (1024*1024):.2f}MB)")
                await self.send_message(
                    self.owner_id,
                    f"❌ **Upload Failed**\n"
                    f"📁 File: {file_path.name}\n"
                    f"🚫 Error: File size ({file_size / (1024*1024):.2f}MB) exceeds limit ({MAX_FILE_SIZE_MB}MB)"
                )
                return False

            file_path = await self._rename_with_suffix(file_path)
            if not self.thumbnail_path:
                self.thumbnail_path = await get_thumbnail()

            caption = (
                f"**{file_path.stem.replace(SUFFIX, '')}**\n\n"
                f"📁 Size: {file_size / (1024*1024):.2f} MB\n"
                f"🎥 Video Torrent Download\n"
                f"⚡ Powered by @VideoDLBot"
            )

            logging.info(f"Uploading {file_path.name} to Telegram...")
            await self.send_document(
                chat_id=self.owner_id,
                document=str(file_path),
                caption=caption,
                thumb=str(self.thumbnail_path) if self.thumbnail_path else None,
                progress=self._upload_progress
            )
            logging.info(f"Successfully uploaded {file_path.name}")

            try:
                file_path.unlink()
                if file_path.parent != DOWNLOAD_DIR and not any(file_path.parent.iterdir()):
                    file_path.parent.rmdir()
            except Exception as e:
                logging.warning(f"Failed to clean up file {file_path}: {e}")

            self.db.move_to_completed(download.gid)
            return True
        except Exception as e:
            logging.error(f"Upload failed for GID {download.gid}: {e}")
            await self.send_message(
                self.owner_id,
                f"❌ **Upload Failed**\n"
                f"📁 File: {getattr(download, 'name', 'Unknown')}\n"
                f"🚫 Error: Check logs for details"
            )
            return False

    async def _upload_progress(self, current, total):
        try:
            percent = current * 100 / total
            if int(percent) % 20 == 0 or percent == 100 or percent == 0:
                if not hasattr(self, '_last_logged_upload_percent') or int(percent / 20) > int(self._last_logged_upload_percent / 20) or percent == 100:
                    logging.info(f"Upload progress: {percent:.1f}%")
                    self._last_logged_upload_percent = percent
        except Exception as e:
            logging.debug(f"Upload progress logging error: {e}")

    async def _process_queue(self):
        try:
            queued = self.db.get_queued_items()
            logging.info(f"Processing {len(queued)} queued items from DB")
            for item in queued:
                if not self.db.is_completed(item['content']):
                    is_active = any(active_dl['content'] == item['content'] for active_dl in self.active_downloads.values())
                    if not is_active:
                        await self.download_queue.put((item['content'], item['title']))
                    else:
                        logging.info(f"Skipping DB queued item '{item['title']}' - already active")
                else:
                    logging.info(f"Skipping DB queued item '{item['title']}' - already completed")
        except Exception as e:
            logging.error(f"Error processing queue from DB: {e}")

    async def _recover_active(self):
        try:
            self.db.cleanup_stale_active()
            active = self.db.get_active_downloads()
            logging.info(f"Recovering {len(active)} active downloads from DB")
            for item in active:
                try:
                    if not self.aria2:
                        logging.warning(f"Aria2 not available, re-adding {item['title']} to queue")
                        await self.download_queue.put((item['content'], item['title']))
                        self.db.active.delete_one({"gid": item['gid']})
                        continue

                    download = self.aria2.get_download(item['gid'])
                    if download.is_complete:
                        logging.info(f"Found completed download: {item['title']}")
                        await self._upload_file(download)
                    elif download.is_active:
                        self.active_downloads[download.gid] = {
                            'title': item['title'],
                            'content': item['content'],
                            'start_time': item['started_at'].timestamp() if 'started_at' in item else time.time()
                        }
                        logging.info(f"Recovered active download: {item['title']} (GID: {download.gid})")
                    elif download.status in ["paused", "waiting"]:
                        download.resume()
                        self.active_downloads[download.gid] = {
                            'title': item['title'],
                            'content': item['content'],
                            'start_time': item['started_at'].timestamp() if 'started_at' in item else time.time()
                        }
                    else:
                        logging.info(f"Download {item['title']} (GID: {item['gid']}) in unexpected state, removing")
                        self.db.active.delete_one({"gid": item['gid']})
                except aria2p.ClientException as e:
                    if "not found" in str(e).lower():
                        logging.info(f"GID {item['gid']} not found, re-adding {item['title']} to queue")
                        self.db.active.delete_one({"gid": item['gid']})
                        await self.download_queue.put((item['content'], item['title']))
                    else:
                        logging.error(f"Aria2 error recovering {item['gid']}: {e}")
                except Exception as e:
                    logging.error(f"Error recovering {item['gid']}: {e}")
        except Exception as e:
            logging.error(f"Error during active downloads recovery: {e}")

    async def _download_worker(self):
        while self.running:
            try:
                try:
                    content, title = await asyncio.wait_for(self.download_queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    await asyncio.sleep(1)
                    continue

                if self.db.is_completed(content):
                    logging.info(f"Skipping download '{title}' - already completed")
                    self.download_queue.task_done()
                    continue

                is_already_active = any(active_dl['content'] == content for active_dl in self.active_downloads.values())
                if is_already_active:
                    logging.info(f"Skipping download '{title}' - already active")
                    self.download_queue.task_done()
                    continue

                if not self.aria2:
                    logging.error(f"Aria2 not initialized, requeuing {title}")
                    await self.download_queue.put((content, title))
                    self.download_queue.task_done()
                    await self.send_message(self.owner_id, f"⚠️ **Aria2 Not Ready**\nDownload for '{title[:50]}...' pending")
                    await asyncio.sleep(30)
                    continue

                try:
                    if not (content.startswith('magnet:') or content.endswith('.torrent')):
                        logging.warning(f"Invalid torrent link for '{title}': {content}")
                        await self.send_message(self.owner_id, f"❌ **Invalid Link**\n'{title[:50]}...': Not a torrent or magnet")
                        self.download_queue.task_done()
                        continue

                    logging.info(f"Starting download: {title}")
                    if content.startswith('magnet:'):
                        download = self.aria2.add_magnet(content, {"dir": str(DOWNLOAD_DIR)})
                    else:
                        download = self.aria2.add_uri([content], {"dir": str(DOWNLOAD_DIR)})

                    self.db.move_to_active(download.gid, content, title)
                    self.active_downloads[download.gid] = {
                        'title': title,
                        'content': content,
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
                    logging.error(f"Download failed for '{title}': {e}")
                    await self.send_message(
                        self.owner_id,
                        f"❌ **Download Failed**\n"
                        f"📁 {title[:50]}{'...' if len(title) > 50 else ''}\n"
                        f"🚫 Error: Check logs"
                    )
                finally:
                    self.download_queue.task_done()
            except Exception as e:
                logging.error(f"Download worker error: {e}")
                await asyncio.sleep(5)

    async def _monitor_downloads(self):
        while self.running:
            try:
                await asyncio.sleep(DOWNLOAD_STATUS_UPDATE_INTERVAL)
                if not self.aria2:
                    logging.warning("Aria2 not available for monitoring")
                    await asyncio.sleep(5)
                    continue

                for gid in list(self.active_downloads.keys()):
                    if not self.running:
                        break
                    try:
                        download = self.aria2.get_download(gid)
                        if download.is_complete:
                            logging.info(f"Download completed: {self.active_downloads[gid]['title']}")
                            if await self._upload_file(download):
                                await self.send_message(
                                    self.owner_id,
                                    f"✅ **Upload Complete**\n"
                                    f"📁 {self.active_downloads[gid]['title'][:50]}{'...' if len(self.active_downloads[gid]['title']) > 50 else ''}"
                                )
                            if gid in self.active_downloads:
                                del self.active_downloads[gid]
                        elif download.status == "error":
                            logging.error(f"Download failed for '{self.active_downloads[gid]['title']}': {download.error_message or 'Unknown error'}")
                            await self.send_message(
                                self.owner_id,
                                f"❌ **Download Failed**\n"
                                f"📁 {self.active_downloads[gid]['title'][:50]}{'...' if len(self.active_downloads[gid]['title']) > 50 else ''}\n"
                                f"🚫 Error: Check logs"
                            )
                            self.db.active.delete_one({"gid": gid})
                            if gid in self.active_downloads:
                                del self.active_downloads[gid]
                        elif not download.is_active and download.status not in ["paused", "waiting", "complete"]:
                            logging.warning(f"Download '{self.active_downloads[gid]['title']}' in unexpected state: {download.status}")
                            self.db.active.delete_one({"gid": gid})
                            if gid in self.active_downloads:
                                del self.active_downloads[gid]
                    except aria2p.ClientException as e:
                        if "not found" in str(e).lower():
                            logging.info(f"GID {gid} not found, cleaning up")
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

    @Client.on_message(filters.text & filters.user(OWNER.ID))
    async def handle_torrent_link(self, client, message):
        content = message.text
        title = content.split('/')[-1] if not content.startswith('magnet:') else content.split('&dn=')[1].split('&')[0] if '&dn=' in content else "Video Download"
        if self.db.is_completed(content):
            await message.reply_text(f"✅ **Already Downloaded**\n📁 {title[:50]}{'...' if len(title) > 50 else ''}")
            return
        self.db.add_to_queue(content, title)
        await self.download_queue.put((content, title))
        await message.reply_text(f"📥 **Added to Queue**\n📁 {title[:50]}{'...' if len(title) > 50 else ''}")
        logging.info(f"Added to queue: {title}")

    async def start(self):
        await super().start()
        self.running = True
        self.aria2_process, self.aria2 = await start_aria2()
        if not self.aria2:
            await self.send_message(self.owner_id, "❌ Failed to start Aria2. Downloads won’t work. Check logs.")
        self.thumbnail_path = await get_thumbnail()
        await self._process_queue()
        await self._recover_active()
        asyncio.create_task(self._download_worker())
        asyncio.create_task(self._monitor_downloads())
        await self.send_message(
            self.owner_id,
            f"✅ **Video Torrent Bot Started**\n\n"
            f"🔧 Features:\n"
            f"• Downloads video torrents\n"
            f"• MongoDB tracking\n"
            f"• Thumbnail support\n"
            f"• Auto-recovery\n\n"
            f"📊 Status:\n"
            f"• Active downloads: {len(self.active_downloads)}\n"
            f"• Queue size: {self.download_queue.qsize()}\n\n"
            f"Send a magnet link or torrent URL to start downloading videos!"
        )

    async def stop(self):
        logging.info("Stopping bot...")
        self.running = False
        await asyncio.sleep(2)
        if self.aria2_process:
            logging.info("Terminating aria2 process...")
            try:
                self.aria2_process.terminate()
                self.aria2_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.aria2_process.kill()
            except Exception as e:
                logging.error(f"Error terminating aria2: {e}")
        await super().stop()
        logging.info("Bot stopped.")

# ------------------ Main ------------------
async def main():
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    bot = VideoTorrentBot()
    try:
        await bot.start()
        logging.info("Bot started, waiting for events...")
        await idle()
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.critical(f"Bot crashed: {e}")
        sys.exit(1)
    finally:
        await bot.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Program interrupted by user")
    except Exception as e:
        logging.critical(f"Program crashed: {e}")
        sys.exit(1)
