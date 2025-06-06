import asyncio
import logging
import os
import re
import ssl
import threading
from pathlib import Path
from typing import Dict, List

import aiohttp
import certifi
import feedparser
from bs4 import BeautifulSoup
from flask import Flask
from pymongo import MongoClient
from pyrogram import Client, idle
from config import BOT, API, OWNER

# ------------------ Constants ------------------
DOWNLOAD_DIR = Path(__file__).parent / "downloads"
THUMBNAIL_URL = "https://i.ibb.co/MDwd1f3D/6087047735061627461.jpg"
SUFFIX = " -@MNTGX.-"
MAX_FILE_SIZE_MB = 1900
MONGODB_URI = "mongodb+srv://mntgx:mntgx@cluster0.pzcpq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Sources
ARCHIVE_ORG_FEEDS = [
    "https://archive.org/services/collection-rss.php?collection=feature_films&query=year%3A1920-1980"
]
CLASSIC_CINEMA_URL = "https://www.classiccinemaonline.com/movies/"

# ------------------ Flask ------------------
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    return "Classic Cinema Bot is running!"

def run_flask():
    flask_app.run(host='0.0.0.0', port=8000)

# ------------------ MongoDB ------------------
class MongoDB:
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client.classic_cinema_bot
        self.completed = self.db.completed
    
    def is_completed(self, content):
        return bool(self.completed.find_one({"content": content}))
    
    def mark_completed(self, content):
        self.completed.insert_one({"content": content})

# ------------------ Utilities ------------------
def sanitize_filename(filename: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", filename)

async def create_ssl_context():
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    ssl_context.check_hostname = False  # Disable hostname verification
    ssl_context.verify_mode = ssl.CERT_NONE  # Disable certificate verification
    return ssl_context

async def download_file(url: str, save_path: Path) -> bool:
    ssl_context = await create_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url) as response:
            if response.status == 200:
                with open(save_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(1024):
                        f.write(chunk)
                return True
    return False

async def get_movie_metadata(title: str) -> Dict:
    year_match = re.search(r'(19\d{2}|20\d{2})', title)
    return {
        "title": title,
        "year": year_match.group(1) if year_match else "N/A",
        "source": "Archive.org" if "archive.org" in title.lower() else "ClassicCinemaOnline"
    }

# ------------------ Scrapers ------------------
async def scrape_archive_org() -> List[Dict]:
    movies = []
    for feed_url in ARCHIVE_ORG_FEEDS:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            if not db.is_completed(entry.link):
                movies.append({
                    "title": entry.title,
                    "url": entry.link,
                    "download_url": entry.link.replace('/details/', '/download/') + "/" + entry.title.replace(' ', '_') + ".mp4"
                })
    return movies

async def scrape_classic_cinema() -> List[Dict]:
    movies = []
    ssl_context = await create_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(CLASSIC_CINEMA_URL) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    for movie_div in soup.find_all('div', class_='movie-item'):
                        title = movie_div.find('h3').text.strip()
                        page_url = movie_div.find('a')['href']
                        if not db.is_completed(page_url):
                            async with session.get(page_url) as movie_page:
                                if movie_page.status == 200:
                                    movie_soup = BeautifulSoup(await movie_page.text(), 'html.parser')
                                    if download_div := movie_soup.find('div', class_='download-link'):
                                        movies.append({
                                            "title": title,
                                            "url": page_url,
                                            "download_url": download_div.find('a')['href']
                                        })
    except Exception as e:
        logging.error(f"Error scraping classic cinema: {e}")
    return movies

# ------------------ Bot ------------------
class ClassicCinemaBot(Client):
    def __init__(self):
        super().__init__(
            "ClassicCinemaBot",
            api_id=Config.API.ID,
            api_hash=Config.API.HASH,
            bot_token=Config.BOT.TOKEN
        )
        self.db = MongoDB()
        self.thumbnail_path = None

    async def start(self):
        await super().start()
        self.thumbnail_path = await self.download_thumbnail()
        threading.Thread(target=run_flask, daemon=True).start()
        asyncio.create_task(self.scrape_and_process_loop())
        await self.send_message(Config.OWNER.ID, "✅ Classic Cinema Bot Started!")

    async def download_thumbnail(self):
        path = Path("thumbnail.jpg")
        if path.exists():
            return path
        
        ssl_context = await create_ssl_context()
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(THUMBNAIL_URL) as response:
                    if response.status == 200:
                        with open(path, "wb") as f:
                            async for chunk in response.content.iter_chunked(1024):
                                f.write(chunk)
                        return path
        except Exception as e:
            logging.error(f"Failed to download thumbnail: {e}")
            return None

    async def scrape_and_process_loop(self):
        while True:
            try:
                archive_movies = await scrape_archive_org()
                classic_movies = await scrape_classic_cinema()
                all_movies = archive_movies + classic_movies

                for movie in all_movies:
                    if not self.db.is_completed(movie['url']):
                        await self.process_movie(movie)
                        await asyncio.sleep(10)

            except Exception as e:
                logging.error(f"Error in scrape loop: {e}")
                await self.send_message(Config.OWNER.ID, f"❌ Scrape Error: {str(e)}")

            await asyncio.sleep(3600)

    async def process_movie(self, movie: Dict):
        try:
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            clean_title = sanitize_filename(movie['title'])
            file_ext = Path(movie['download_url']).suffix or '.mp4'
            filename = f"{clean_title[:50]}{SUFFIX}{file_ext}"
            save_path = DOWNLOAD_DIR / filename

            await self.send_message(Config.OWNER.ID, f"⬇️ Downloading: {movie['title']}")
            success = await download_file(movie['download_url'], save_path)
            
            if success:
                metadata = await get_movie_metadata(movie['title'])
                caption = f"🎬 {metadata['title']}\n📅 Year: {metadata['year']}\n🏷️ Source: {metadata['source']}"

                file_size = save_path.stat().st_size / (1024 * 1024)
                if file_size > MAX_FILE_SIZE_MB:
                    await self.send_message(Config.OWNER.ID, f"❌ File too big ({file_size:.2f}MB): {filename}")
                    save_path.unlink()
                    return

                await self.send_document(
                    chat_id=Config.OWNER.ID,
                    document=str(save_path),
                    caption=caption,
                    thumb=str(self.thumbnail_path) if self.thumbnail_path else None
                )

                save_path.unlink()
                self.db.mark_completed(movie['url'])
                await self.send_message(Config.OWNER.ID, f"✅ Successfully sent: {movie['title']}")

            else:
                await self.send_message(Config.OWNER.ID, f"❌ Failed to download: {movie['title']}")

        except Exception as e:
            await self.send_message(Config.OWNER.ID, f"❌ Error processing {movie['title']}: {str(e)}")
            logging.error(f"Error processing movie: {e}")

# ------------------ Main ------------------
async def main():
    global db
    db = MongoDB()
    bot = ClassicCinemaBot()
    await bot.start()
    await idle()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
