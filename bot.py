import asyncio
import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp
import feedparser
import qbittorrentapi
from flask import Flask
from PIL import Image
from pymongo import MongoClient
from pyrogram import Client, filters, idle
from config import BOT, API, OWNER

# ------------------ Constants ------------------
DOWNLOAD_DIR = Path(__file__).parent / "downloads"
THUMBNAIL_URL = "https://i.ibb.co/MDwd1f3D/6087047735061627461.jpg"
SUFFIX = " -@MNTGX.-"
MAX_FILE_SIZE_MB = 1900
PSA_FEEDS = ["https://bt4gprx.com/search?q=psa&page=rss"]
MONGODB_URI = "your_mongo_uri"

# ------------------ Flask ------------------
app = Flask(__name__)
@app.route('/')
def home(): return "Bot is running!"
def run_flask(): app.run(host='0.0.0.0', port=8000)

# ------------------ MongoDB ------------------
class MongoDB:
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client.torrent_bot
        self.completed = self.db.completed
        self.queue = self.db.queue
        self.active = self.db.active
    def is_completed(self, content): return bool(self.completed.find_one({"content": content}))
    def add_to_queue(self, content, title): self.queue.insert_one({"content": content, "title": title})
    def get_queued_items(self): return list(self.queue.find())

# ------------------ Utilities ------------------
def add_suffix(filename: str) -> str:
    base, ext = os.path.splitext(filename)
    name = f"{base[:50]}{SUFFIX}{ext}"
    return name if len(name) <= 60 else f"{base[:40]}{SUFFIX}{ext}"

def get_largest_file(folder: Path) -> Optional[Path]:
    files = list(folder.rglob("*"))
    files = [f for f in files if f.suffix in [".mp4", ".mkv", ".avi"]]
    return max(files, key=lambda f: f.stat().st_size) if files else None

async def download_thumbnail():
    path = Path("thumbnail.jpg")
    if path.exists(): return path
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(THUMBNAIL_URL) as r:
                if r.status == 200:
                    with open(path, "wb") as f: f.write(await r.read())
                    return path
    except: return None

# ------------------ Bot ------------------
class TorrentBot(Client):
    def __init__(self):
        super().__init__("TorrentBot", api_id=API.ID, api_hash=API.HASH, bot_token=BOT.TOKEN)
        self.db = MongoDB()
        self.qb = qbittorrentapi.Client(host="localhost", port=8080, username="admin", password="adminadmin")
        self.thumbnail_path = None
        self.download_queue = asyncio.Queue()

    async def start(self):
        try:
            self.qb.auth_log_in()
        except Exception as e:
            print(f"qBittorrent login failed: {e}")
        await super().start()
        self.thumbnail_path = await download_thumbnail()
        threading.Thread(target=run_flask, daemon=True).start()
        asyncio.create_task(self.feed_loop())
        asyncio.create_task(self.monitor_qb())
        await self.send_message(OWNER.ID, "✅ Bot Started!")

    async def monitor_qb(self):
        while True:
            await asyncio.sleep(10)
            for torrent in self.qb.torrents_info():
                if torrent.state == "uploading":
                    file = get_largest_file(Path(torrent.content_path))
                    if file:
                        await self.upload_file(file)
                        self.qb.torrents_delete(torrent_hashes=torrent.hash, delete_files=False)

    async def upload_file(self, file: Path):
        size = file.stat().st_size
        if size > MAX_FILE_SIZE_MB * 1024 * 1024:
            await self.send_message(OWNER.ID, f"❌ File too big: {file.name}")
            return
        renamed = file.with_name(add_suffix(file.name))
        file.rename(renamed)
        await self.send_document(OWNER.ID, document=str(renamed), caption=renamed.name,
                                 thumb=str(self.thumbnail_path) if self.thumbnail_path else None)

    async def feed_loop(self):
        while True:
            for url in PSA_FEEDS:
                feed = feedparser.parse(url)
                for entry in feed.entries:
                    link = entry.link
                    if not self.db.is_completed(link):
                        self.db.add_to_queue(link, entry.title)
                        self.qb.torrents_add(urls=link, save_path=str(DOWNLOAD_DIR))
                        await self.send_message(OWNER.ID, f"📥 Added: {entry.title}")
                        await asyncio.sleep(1)
            await asyncio.sleep(600)

# ------------------ Main ------------------
async def main():
    bot = TorrentBot()
    await bot.start()
    await idle()

if __name__ == "__main__":
    asyncio.run(main())
