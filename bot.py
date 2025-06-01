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
from datetime import datetime
from flask import Flask
from pyrogram import Client, errors
from pyrogram.types import Message
from config import BOT, API, OWNER, CHANNEL
import aiohttp
from PIL import Image

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
MAX_FILE_SIZE_MB = 1900
FILTER_KEYWORDS = [
    "predvd", "prehd", "hdts", "camrip", "telesync", "screener", "ts", "dvdscr"
]
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
THUMBNAIL_DIR = "thumbnails"
os.makedirs(THUMBNAIL_DIR, exist_ok=True)
TELEGRAM_FILENAME_LIMIT = 64
MAGNET_RESPONSE_TIMEOUT_SECONDS = 600  # 10 minutes

# ------------------ Utility Functions ------------------
# [fetch_feed, download_torrent_or_link, format_message, is_valid_entry, download_thumbnail, create_default_thumbnail, add_suffix_to_filename, keep_alive remain unchanged]

# ------------------ Bot Class ------------------
class MN_Bot(Client):
    MAX_MSG_LENGTH = 4000
    DOWNLOAD_STATUS_UPDATE_INTERVAL = 5  # seconds

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
        self.thumbnail_path = None
        self.active_downloads = {}  # {gid: {'process': subprocess.Popen, 'message': pyrogram_message_object, 'entry_title': str, 'total_size_bytes': int, 'last_update_time': float}}
        self.download_queue = asyncio.Queue()

    async def _add_torrent_to_session(self, content: str, entry_title: str):
        """Adds a magnet link or .torrent file to aria2c for downloading."""
        try:
            # Prepare aria2c command
            aria2c_cmd = [
                "aria2c",
                "--dir", DOWNLOAD_DIR,
                "--seed-time=0",  # Disable seeding
                "--max-overall-upload-limit=1K",
                "--bt-stop-timeout=1800",  # Stop if no peers in 30 minutes
                "--summary-interval=5",  # Update interval for progress
                "--enable-color=false",  # Disable color output for parsing
                content  # Magnet link or .torrent file path
            ]

            # Start aria2c process
            process = subprocess.Popen(
                aria2c_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )

            # Initialize status message
            initial_message_text = f"**Starting Download:** `{entry_title}`\n" \
                                  f"**Total Size:** `Unknown`\n" \
                                  f"**Progress:** `0.00%`\n" \
                                  f"**Downloaded:** `0.00 MB`\n" \
                                  f"**Speed:** `0.00 KB/s`\n" \
                                  f"**Status:** `Starting...`"
            status_message = await self.send_message(self.owner_id, initial_message_text)

            # Store process info (using process PID as a unique identifier)
            gid = str(process.pid)
            self.active_downloads[gid] = {
                'process': process,
                'message': status_message,
                'entry_title': entry_title,
                'total_size_bytes': 0,  # Will be updated when known
                'last_update_time': time.time(),
                'output_dir': os.path.join(DOWNLOAD_DIR, entry_title.replace("/", "_"))  # Safe directory name
            }
            logging.info(f"Started aria2c download for '{entry_title}'. PID: {gid}")
            return gid
        except Exception as e:
            logging.error(f"Error starting aria2c for {entry_title}: {e}")
            await self.send_message(self.owner_id, f"❌ **Failed to Start Download:** `{entry_title}`\nError: `{e}`")
            return None

    async def _update_download_progress(self):
        """Periodically checks aria2c process output for progress updates."""
        while True:
            await asyncio.sleep(self.DOWNLOAD_STATUS_UPDATE_INTERVAL)
            for gid, data in list(self.active_downloads.items()):
                process = data['process']
                entry_title = data['entry_title']
                status_message = data['message']

                # Check if process has completed
                returncode = process.poll()
                if returncode is not None:
                    if returncode == 0:
                        logging.info(f"Download of '{entry_title}' completed!")
                        final_message_text = f"✅ **Download Completed:** `{entry_title}`\n" \
                                            f"**Total Size:** `{data.get('total_size_bytes', 0) / (1024*1024):.2f} MB`"
                        try:
                            await status_message.edit_text(final_message_text)
                            await self._send_downloaded_file_to_channel(gid, entry_title, data['output_dir'])
                        except errors.MessageNotModified:
                            pass
                        except Exception as e:
                            logging.error(f"Error processing completion for {entry_title}: {e}")
                        del self.active_downloads[gid]
                    else:
                        logging.error(f"Download failed for '{entry_title}'. Return code: {returncode}")
                        error_message_text = f"❌ **Download Failed:** `{entry_title}`\nError: `Process exited with code {returncode}`"
                        try:
                            await status_message.edit_text(error_message_text)
                        except errors.MessageNotModified:
                            pass
                        except Exception as e:
                            logging.error(f"Error editing error message for {entry_title}: {e}")
                        del self.active_downloads[gid]
                    continue

                # Parse aria2c output for progress
                try:
                    # Read recent output from stderr (aria2c logs progress to stderr)
                    output = ""
                    while process.stderr and not process.stderr._file.closed:
                        line = process.stderr.readline().strip()
                        if not line:
                            break
                        output += line + "\n"

                    # Extract progress, size, and speed using regex
                    progress_match = re.search(r"\[.*?\((\d+)%\).*?(\d+/\d+MiB|Unknown).*?(\d+\.?\d*KiB/s)?", output)
                    if progress_match:
                        progress = float(progress_match.group(1))
                        size_str = progress_match.group(2).split("/")[0] if "/" in progress_match.group(2) else "0"
                        downloaded_mb = float(re.search(r"(\d+)", size_str).group(1)) if re.search(r"(\d+)", size_str) else 0
                        total_size_mb = float(re.search(r"(\d+)", progress_match.group(2).split("/")[1]).group(1)) if "/" in progress_match.group(2) else 0
                        total_size_bytes = total_size_mb * 1024 * 1024
                        speed_kbps = float(re.search(r"(\d+\.?\d*)", progress_match.group(3)).group(1)) if progress_match.group(3) else 0

                        # Update total size in active_downloads
                        data['total_size_bytes'] = total_size_bytes

                        # Check size limit
                        if total_size_mb > MAX_FILE_SIZE_MB:
                            logging.info(f"Stopping download of '{entry_title}' due to large size ({total_size_mb:.2f} MB).")
                            process.terminate()
                            await self.send_message(self.owner_id, f"🚫 **Skipped:** `{entry_title}`\nSize ({total_size_mb:.2f} MB) exceeds maximum allowed ({MAX_FILE_SIZE_MB} MB).")
                            del self.active_downloads[gid]
                            continue

                        progress_bar = self._create_progress_bar(progress)
                        updated_message_text = f"**Downloading:** `{entry_title}`\n" \
                                              f"**Total Size:** `{total_size_mb:.2f} MB`\n" \
                                              f"**Progress:** `{progress:.2f}%` {progress_bar}\n" \
                                              f"**Downloaded:** `{downloaded_mb:.2f} MB`\n" \
                                              f"**Speed:** `{speed_kbps:.2f} KB/s`\n" \
                                              f"**Status:** `Active`"
                        try:
                            if time.time() - data['last_update_time'] > self.DOWNLOAD_STATUS_UPDATE_INTERVAL:
                                await status_message.edit_text(updated_message_text)
                                data['last_update_time'] = time.time()
                        except errors.MessageNotModified:
                            pass
                        except Exception as e:
                            logging.error(f"Error editing message for {entry_title}: {e}")
                except Exception as e:
                    logging.error(f"Error parsing aria2c output for {entry_title}: {e}")

    async def _send_downloaded_file_to_channel(self, gid: str, entry_title: str, output_dir: str):
        try:
            # Find the downloaded files in the output directory
            files = []
            for root, _, filenames in os.walk(output_dir):
                for filename in filenames:
                    file_path = os.path.join(root, filename)
                    file_size = os.path.getsize(file_path)
                    files.append({'path': file_path, 'size': file_size})

            if not files:
                logging.warning(f"No files found in output directory for {entry_title}.")
                await self.send_message(self.owner_id, f"❌ No files found for '{entry_title}'.")
                return

            files.sort(key=lambda x: x['size'], reverse=True)
            largest_file = files[0]
            file_path = largest_file['path']

            if largest_file['size'] > 2 * 1024 * 1024 * 1024:
                logging.warning(f"File '{largest_file['path']}' is too large ({largest_file['size'] / (1024*1024*1024):.2f} GB) for Telegram.")
                await self.send_message(self.owner_id, f"⚠️ Downloaded file '{entry_title}' is too large to upload to Telegram ({largest_file['size'] / (1024*1024*1024):.2f} GB).")
                os.remove(file_path)
                logging.info(f"Removed large downloaded file: {file_path}")
                return

            # Add suffix to filename
            file_path = add_suffix_to_filename(file_path)

            # Use the downloaded custom thumbnail
            if not self.thumbnail_path:
                self.thumbnail_path = await download_thumbnail()
                if not self.thumbnail_path:
                    self.thumbnail_path = create_default_thumbnail()

            logging.info(f"Uploading '{os.path.basename(file_path)}' ({largest_file['size'] / (1024*1024):.2f} MB) to channel as document...")
            await self.send_document(
                chat_id=self.channel_id,
                document=file_path,
                caption=f"**{entry_title}**\n\n_Downloaded by bot._",
                thumb=self.thumbnail_path if self.thumbnail_path and os.path.exists(self.thumbnail_path) else None,
                force_document=True
            )
            logging.info(f"Successfully uploaded '{os.path.basename(file_path)}' to channel as document.")

            # Clean up files
            os.remove(file_path)
            logging.info(f"Removed downloaded file: {file_path}")

        except Exception as e:
            logging.error(f"Error sending downloaded file to channel for {entry_title}: {e}")
            await self.send_message(self.owner_id, f"❌ Failed to upload '{entry_title}' to channel. Error: {e}")

    def _create_progress_bar(self, progress: float, bar_length: int = 20) -> str:
        filled_length = int(bar_length * progress // 100)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        return f"[{bar}]"

    async def send_safe(self, chat_id, content, content_type, entry):
        if content_type == 'link' and "magnet:" in content:
            await self.download_queue.put((content, entry.title))
            logging.info(f"Added '{entry.title}' to download queue.")
            await self.send_message(self.owner_id, f"📥 Queued for download: `{entry.title}`")
        elif content_type == 'torrent':
            try:
                resp = requests.get(content, timeout=10)
                resp.raise_for_status()
                torrent_file_path = os.path.join(DOWNLOAD_DIR, f"{entry.title.replace('/', '_')}.torrent")
                with open(torrent_file_path, 'wb') as f:
                    f.write(resp.content)
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
        """Worker to process download queue sequentially."""
        while True:
            item = await self.download_queue.get()
            content, entry_title = item
            logging.info(f"Processing download from queue for: {entry_title}")
            if content.startswith("magnet:") or content.endswith(".torrent"):
                await self._add_torrent_to_session(content, entry_title)
            else:
                logging.warning(f"Unknown item in download queue: {item}")
            self.download_queue.task_done()

    async def auto_post(self):
        while True:
            try:
                for url in PSA_FEEDS:
                    await self.process_psa_feed(url)
                    await asyncio.sleep(5)
                await asyncio.sleep(300)  # 5 minutes
            except Exception as e:
                logging.error(f"Error in auto_post: {e}")
                await asyncio.sleep(60)

    async def start(self):
        await super().start()
        me = await self.get_me()
        BOT.USERNAME = f"@{me.username}"
        self.thumbnail_path = await download_thumbnail()
        if not self.thumbnail_path:
            self.thumbnail_path = create_default_thumbnail()
        asyncio.create_task(keep_alive())
        asyncio.create_task(self._update_download_progress())
        asyncio.create_task(self._download_worker())
        await self.initial_post()
        asyncio.create_task(self.auto_post())
        await self.send_message(self.owner_id, f"{me.first_name} ✅ Bot started and torrent monitoring active. Processing downloads sequentially.")

    async def stop(self, *args):
        logging.info("Stopping all active downloads...")
        for gid, data in self.active_downloads.items():
            try:
                data['process'].terminate()
            except Exception as e:
                logging.error(f"Error terminating download {gid}: {e}")
        self.active_downloads.clear()
        await super().stop()
        logging.info("Bot stopped")

# ------------------ Main ------------------
if __name__ == "__main__":
    threading.Thread(target=run_flask).start()
    MN_Bot().run()
