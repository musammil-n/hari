import asyncio
import logging
import os
import re
import ssl
import threading
from pathlib import Path
from typing import Dict, List
from datetime import datetime

import aiohttp
import certifi
import feedparser
import requests
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
MIN_FILE_SIZE_MB = 100  # Added minimum file size
MONGODB_URI = "mongodb+srv://mntgx:mntgx@cluster0.pzcpq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Sources
ARCHIVE_ORG_FEEDS = [
    "https://archive.org/services/collection-rss.php?mediatype=movies"
]
SKYMOVIESHD_BASE_URL = "https://skymovieshd.dance"

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

def extract_years_from_keywords(keywords):
    if not keywords:
        return None
    return ", ".join(sorted(set(re.findall(r"\b(19\d{2}|20\d{2})\b", keywords))))

async def create_ssl_context():
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
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

async def get_movie_metadata(title: str, keywords: str = "", source: str = "Unknown") -> Dict:
    year_match = re.search(r'(19\d{2}|20\d{2})', title)
    years = extract_years_from_keywords(keywords)
    return {
        "title": title,
        "year": year_match.group(1) if year_match else "N/A",
        "years": years if years else "N/A",
        "source": source
    }

# ------------------ Scrapers ------------------
async def scrape_archive_org() -> List[Dict]:
    movies = []
    for feed_url in ARCHIVE_ORG_FEEDS:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            if not db.is_completed(entry.link):
                # Get video links from media_content
                video_links = [
                    media["url"] for media in entry.get("media_content", [])
                    if media.get("type", "").startswith("video/") and "url" in media
                ]
                
                if video_links:
                    movies.append({
                        "title": entry.title,
                        "url": entry.link,
                        "download_url": video_links[0],  # Use first video link
                        "keywords": entry.get("media_keywords", ""),
                        "source": "Archive.org"
                    })
    return movies

async def skymovieshd_scrape_movie_page(url: str) -> List[str]:
    """Scrapes a SkyMoviesHD movie page for direct download links."""
    try:
        res = requests.get(url, allow_redirects=False)
        res.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        download_links = []
        _cache = []

        # Find links that lead to howblogs.xyz, which then contain the actual download links
        for link in soup.select('a[href*="howblogs.xyz"]'):
            if link['href'] in _cache:
                continue
            _cache.append(link['href'])
            
            try:
                resp = requests.get(link['href'], allow_redirects=False)
                resp.raise_for_status()
                nsoup = BeautifulSoup(resp.text, 'html.parser') 
                atag = nsoup.select('div[class="cotent-box"] > a[href]')
                for d_link in atag: 
                    download_links.append(d_link['href'])
            except requests.exceptions.RequestException as e:
                logging.warning(f"Failed to fetch or parse howblogs.xyz link {link['href']}: {e}")
                continue
        return download_links
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch or parse SkyMoviesHD movie page {url}: {e}")
        return []

async def scrape_skymovieshd_recent() -> List[Dict]:
    """Scrapes the main SkyMoviesHD page for recent movie links."""
    movies = []
    try:
        res = requests.get(SKYMOVIESHD_BASE_URL, allow_redirects=False)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        # Look for links within the main content area that point to movie pages
        # This selector might need adjustment if SkyMoviesHD's structure changes.
        # It targets links that have a title attribute (often used for movie titles)
        # and are typically found within list items or similar structures.
        for a_tag in soup.select('a[href*="/movie/"][title]'):
            movie_url = a_tag['href']
            if not movie_url.startswith('http'):
                movie_url = SKYMOVIESHD_BASE_URL + movie_url

            if not db.is_completed(movie_url):
                title = a_tag.get('title', 'No Title Found').strip()
                # Attempt to get a more specific title from the text content if available
                if a_tag.text and len(a_tag.text.strip()) > len(title):
                    title = a_tag.text.strip()
                
                # Clean up the title a bit, removing common extra info
                title = re.sub(r'\s*\(?\d+p\)?.*', '', title).strip() # Remove quality info
                title = re.sub(r'\[\d+MB\]', '', title).strip() # Remove size info

                movies.append({
                    "title": title,
                    "url": movie_url,
                    "source": "SkyMoviesHD"
                })
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch or parse SkyMoviesHD main page: {e}")
    return movies


# ------------------ Bot ------------------
class ClassicCinemaBot(Client):
    def __init__(self):
        super().__init__(
            "ClassicCinemaBot",
            api_id=API.ID,
            api_hash=API.HASH,
            bot_token=BOT.TOKEN
        )
        self.db = MongoDB()
        self.thumbnail_path = None

    async def start(self):
        await super().start()
        self.thumbnail_path = await self.download_thumbnail()
        threading.Thread(target=run_flask, daemon=True).start()
        asyncio.create_task(self.scrape_and_process_loop())
        await self.send_message(OWNER.ID, "✅ Classic Cinema Bot Started!")

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
                skymovieshd_movies_meta = await scrape_skymovieshd_recent()

                all_movies_to_process = []
                for movie_meta in skymovieshd_movies_meta:
                    if not self.db.is_completed(movie_meta['url']):
                        download_links = await skymovieshd_scrape_movie_page(movie_meta['url'])
                        if download_links:
                            for dl_link in download_links:
                                all_movies_to_process.append({
                                    "title": movie_meta['title'],
                                    "url": movie_meta['url'], # Original movie page URL for completion tracking
                                    "download_url": dl_link,
                                    "source": movie_meta['source']
                                })
                                # Mark the main movie page as completed once its download links are extracted
                                self.db.mark_completed(movie_meta['url']) 
                        else:
                            logging.info(f"No download links found for SkyMoviesHD movie: {movie_meta['title']} ({movie_meta['url']})")

                all_movies_to_process.extend(archive_movies) # Add archive movies to the list

                for movie in all_movies_to_process:
                    # Check completion status again before processing the actual download URL if it's from SkyMoviesHD
                    # For Archive.org, we already checked entry.link
                    if movie['source'] == "SkyMoviesHD" and self.db.is_completed(movie['download_url']):
                        continue

                    await self.process_movie(movie)
                    # Add a small delay between processing each movie to avoid overwhelming the system
                    await asyncio.sleep(10)

            except Exception as e:
                logging.error(f"Error in scrape loop: {e}")
                await self.send_message(OWNER.ID, f"❌ Scrape Error: {str(e)}")

            await asyncio.sleep(3600) # Wait for an hour before the next scrape cycle

    async def process_movie(self, movie: Dict):
        try:
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            clean_title = sanitize_filename(movie['title'])
            file_ext = Path(movie['download_url']).suffix or '.mp4'
            filename = f"{clean_title[:50]}{SUFFIX}{file_ext}"
            save_path = DOWNLOAD_DIR / filename

            await self.send_message(OWNER.ID, f"⬇️ Downloading: {movie['title']} from {movie['source']}")
            success = await download_file(movie['download_url'], save_path)
            
            if success:
                file_size = save_path.stat().st_size / (1024 * 1024)
                if file_size > MAX_FILE_SIZE_MB:
                    await self.send_message(OWNER.ID, f"❌ File too big ({file_size:.2f}MB): {filename}")
                    save_path.unlink()
                    return
                if file_size < MIN_FILE_SIZE_MB:  # Check minimum file size
                    await self.send_message(OWNER.ID, f"❌ File too small ({file_size:.2f}MB): {filename}")
                    save_path.unlink()
                    return

                metadata = await get_movie_metadata(movie['title'], movie.get('keywords', ''), movie['source'])
                caption = (f"🎬 {metadata['title']}\n"
                          f"📅 Year: {metadata['year']}\n"
                          f"📆 Additional Years: {metadata['years']}\n"
                          f"🏷️ Source: {metadata['source']}")

                await self.send_document(
                    chat_id=OWNER.ID,
                    document=str(save_path),
                    caption=caption,
                    thumb=str(self.thumbnail_path) if self.thumbnail_path else None
                )

                save_path.unlink()
                # Mark the specific download URL as completed for SkyMoviesHD
                if movie['source'] == "SkyMoviesHD":
                    self.db.mark_completed(movie['download_url'])
                else:
                    self.db.mark_completed(movie['url'])
                await self.send_message(OWNER.ID, f"✅ Successfully sent: {movie['title']}")

            else:
                await self.send_message(OWNER.ID, f"❌ Failed to download: {movie['title']} from {movie['source']}")

        except Exception as e:
            await self.send_message(OWNER.ID, f"❌ Error processing {movie['title']} from {movie['source']}: {str(e)}")
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
