import asyncio
import logging
import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict

import aiohttp
import feedparser
from bs4 import BeautifulSoup
from PIL import Image
from pymongo import MongoClient
from pyrogram import Client, filters, idle
from config import BOT, API, OWNER

# ------------------ Constants ------------------
DOWNLOAD_DIR = Path(__file__).parent / "downloads"
THUMBNAIL_URL = "https://i.ibb.co/MDwd1f3D/6087047735061627461.jpg"  # Default thumbnail
SUFFIX = " -@ClassicCinema.-"
MAX_FILE_SIZE_MB = 1900
MONGODB_URI = "mongodb+srv://mntgx:mntgx@cluster0.pzcpq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Sources
ARCHIVE_ORG_FEEDS = [
    "https://archive.org/services/collection-rss.php?collection=feature_films&query=year%3A1920-1980"
]
CLASSIC_CINEMA_URL = "https://www.classiccinemaonline.com/movies/"

# ------------------ Flask ------------------
app = Flask(__name__)
@app.route('/')
def home(): return "Classic Cinema Bot is running!"
def run_flask(): app.run(host='0.0.0.0', port=8000)

# ------------------ MongoDB ------------------
class MongoDB:
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client.classic_cinema_bot
        self.completed = self.db.completed
    def is_completed(self, content): return bool(self.completed.find_one({"content": content}))
    def mark_completed(self, content): self.completed.insert_one({"content": content})

# ------------------ Utilities ------------------
def sanitize_filename(filename: str) -> str:
    """Remove special characters from filename"""
    return re.sub(r'[\\/*?:"<>|]', "", filename)

async def download_file(url: str, save_path: Path) -> bool:
    """Download file with aiohttp"""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                with open(save_path, 'wb') as f:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
                return True
    return False

async def get_movie_metadata(title: str) -> Dict:
    """Get basic metadata for caption"""
    return {
        "title": title,
        "year": re.search(r'(19\d{2}|20\d{2})', title).group(1) if re.search(r'(19\d{2}|20\d{2})', title) else "N/A",
        "source": "Archive.org" if "archive.org" in title.lower() else "ClassicCinemaOnline"
    }

# ------------------ Scrapers ------------------
async def scrape_archive_org() -> List[Dict]:
    """Get movies from Archive.org RSS"""
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
    """Scrape ClassicCinemaOnline.com"""
    movies = []
    async with aiohttp.ClientSession() as session:
        async with session.get(CLASSIC_CINEMA_URL) as response:
            if response.status == 200:
                soup = BeautifulSoup(await response.text(), 'html.parser')
                for movie_div in soup.find_all('div', class_='movie-item'):
                    title = movie_div.find('h3').text.strip()
                    page_url = movie_div.find('a')['href']
                    if not db.is_completed(page_url):
                        # Need to visit individual page to find download link
                        async with session.get(page_url) as movie_page:
                            if movie_page.status == 200:
                                movie_soup = BeautifulSoup(await movie_page.text(), 'html.parser')
                                if download_div := movie_soup.find('div', class_='download-link'):
                                    movies.append({
                                        "title": title,
                                        "url": page_url,
                                        "download_url": download_div.find('a')['href']
                                    })
    return movies

# ------------------ Bot ------------------
class ClassicCinemaBot(Client):
    def __init__(self):
        super().__init__("ClassicCinemaBot", api_id=API.ID, api_hash=API.HASH, bot_token=BOT.TOKEN)
        self.db = MongoDB()
        self.thumbnail_path = None
        self.download_queue = asyncio.Queue()

    async def start(self):
        await super().start()
        self.thumbnail_path = await self.download_thumbnail()
        threading.Thread(target=run_flask, daemon=True).start()
        asyncio.create_task(self.scrape_and_process_loop())
        await self.send_message(OWNER.ID, "✅ Classic Cinema Bot Started!")

    async def download_thumbnail(self):
        path = Path("thumbnail.jpg")
        if path.exists(): return path
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(THUMBNAIL_URL) as response:
                    if response.status == 200:
                        with open(path, "wb") as f:
                            f.write(await response.read())
                        return path
        except Exception as e:
            logging.error(f"Failed to download thumbnail: {e}")
            return None

    async def scrape_and_process_loop(self):
        while True:
            try:
                # Get movies from both sources
                archive_movies = await scrape_archive_org()
                classic_movies = await scrape_classic_cinema()
                all_movies = archive_movies + classic_movies

                for movie in all_movies:
                    if not self.db.is_completed(movie['url']):
                        await self.process_movie(movie)
                        await asyncio.sleep(10)  # Rate limiting

            except Exception as e:
                logging.error(f"Error in scrape loop: {e}")
                await self.send_message(OWNER.ID, f"❌ Scrape Error: {str(e)}")

            await asyncio.sleep(3600)  # Check every hour

    async def process_movie(self, movie: Dict):
        """Download and send a movie"""
        try:
            # Create download directory if not exists
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            
            # Generate filename
            clean_title = sanitize_filename(movie['title'])
            file_ext = Path(movie['download_url']).suffix or '.mp4'
            filename = f"{clean_title[:50]}{SUFFIX}{file_ext}"
            save_path = DOWNLOAD_DIR / filename

            # Download the file
            await self.send_message(OWNER.ID, f"⬇️ Downloading: {movie['title']}")
            success = await download_file(movie['download_url'], save_path)
            
            if success:
                # Get metadata for caption
                metadata = await get_movie_metadata(movie['title'])
                caption = (
                    f"🎬 {metadata['title']}\n"
                    f"📅 Year: {metadata['year']}\n"
                    f"🏷️ Source: {metadata['source']}"
                )

                # Check file size
                file_size = save_path.stat().st_size / (1024 * 1024)  # MB
                if file_size > MAX_FILE_SIZE_MB:
                    await self.send_message(OWNER.ID, f"❌ File too big ({file_size:.2f}MB): {filename}")
                    save_path.unlink()
                    return

                # Send to Telegram
                await self.send_document(
                    chat_id=OWNER.ID,
                    document=str(save_path),
                    caption=caption,
                    thumb=str(self.thumbnail_path) if self.thumbnail_path else None
                )

                # Clean up and mark complete
                save_path.unlink()
                self.db.mark_completed(movie['url'])
                await self.send_message(OWNER.ID, f"✅ Successfully sent: {movie['title']}")

            else:
                await self.send_message(OWNER.ID, f"❌ Failed to download: {movie['title']}")

        except Exception as e:
            await self.send_message(OWNER.ID, f"❌ Error processing {movie['title']}: {str(e)}")
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
