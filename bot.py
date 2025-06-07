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
        try:
            async with session.get(url, timeout=300) as response: # Added a 5-minute timeout for downloads
                if response.status == 200:
                    with open(save_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(1024):
                            f.write(chunk)
                    return True
                else:
                    logging.warning(f"Failed to download {url}: HTTP Status {response.status}")
                    return False
        except aiohttp.ClientError as e:
            logging.error(f"AIOHTTP client error during download from {url}: {e}")
            return False
        except asyncio.TimeoutError:
            logging.error(f"Download timed out from {url}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error during download from {url}: {e}")
            return False

def get_quality_info_from_title(title: str) -> str:
    """
    Extracts quality information from a movie title.
    Attempts to find common quality patterns.
    """
    # Look for patterns like "720p HEVC", "1080p BluRay", "HDRip", "WEB-DL"
    match = re.search(r'(\d+p\s*(?:HEVC|BluRay|HDRip|WEB-DL|DVDScr|DVDRip|BRRip|x265|x264|MP4|MKV)?.*?)', title, re.IGNORECASE)
    if match:
        extracted = match.group(1).strip()
        # Further clean up by removing common trailing descriptors that might not be quality
        extracted = re.sub(r'\s*(ORG\.|Dual Audio|ESubs|x26[45]|AAC|DD5\.1|HDR|Esubs)\s*$', '', extracted, flags=re.IGNORECASE).strip()
        return extracted
    
    # Fallback: if no specific pattern found, try to grab content after year
    year_match = re.search(r'\(\d{4}\)\s*(.*)', title)
    if year_match:
        # Take the part after the year, before any square brackets or file extension
        remaining_title = year_match.group(1).split('[')[0].strip()
        remaining_title = remaining_title.split('.')[0].strip() # Remove extension-like parts
        return remaining_title if remaining_title else "Multiple qualities"

    return "Multiple qualities" # Default fallback


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
        try:
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
                            "source": "Archive.org",
                            "all_download_urls": [] # Not applicable for Archive.org, but for consistent dict structure
                        })
        except Exception as e:
            logging.error(f"Error scraping Archive.org feed {feed_url}: {e}")
    return movies

async def skymovieshd_scrape_movie_page(url: str) -> List[str]:
    """Scrapes a SkyMoviesHD movie page for direct download links."""
    try:
        res = requests.get(url, allow_redirects=False, timeout=15) # Added timeout
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
                resp = requests.get(link['href'], allow_redirects=False, timeout=10) # Added timeout
                resp.raise_for_status()
                nsoup = BeautifulSoup(resp.text, 'html.parser') 
                atag = nsoup.select('div[class="cotent-box"] > a[href]')
                for d_link in atag: 
                    download_links.append(d_link['href'])
            except requests.exceptions.Timeout:
                logging.warning(f"Timeout fetching howblogs.xyz link {link['href']}")
                continue
            except requests.exceptions.RequestException as e:
                logging.warning(f"Failed to fetch or parse howblogs.xyz link {link['href']}: {e}")
                continue
        return download_links
    except requests.exceptions.Timeout:
        logging.error(f"Timeout fetching SkyMoviesHD movie page {url}")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch or parse SkyMoviesHD movie page {url}: {e}")
        return []

async def scrape_skymovieshd_recent() -> List[Dict]:
    """Scrapes the main SkyMoviesHD page for recent movie links."""
    movies = []
    try:
        res = requests.get(SKYMOVIESHD_BASE_URL, allow_redirects=False, timeout=15) # Added timeout
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        # Updated selector based on user's input:
        # Find <a> tags within <div class="Fmvideo"> that have hrefs containing "/movie/"
        for a_tag in soup.select('div[class="Fmvideo"] > b > a[href*="/movie/"]'):
            movie_url = a_tag['href']
            # Ensure the URL is absolute
            if not movie_url.startswith('http'):
                movie_url = SKYMOVIESHD_BASE_URL + movie_url

            if not db.is_completed(movie_url):
                title = a_tag.text.strip() # Use the text content of the <a> tag as the title
                
                # Clean up the title a bit, removing common extra info
                # The get_quality_info_from_title function will handle extracting quality
                # from the full title later if needed.
                clean_title = re.sub(r'\[\d+MB\]', '', title).strip() # Remove size info (e.g., "[750MB]")

                movies.append({
                    "title": title, # Store original title for detailed quality info and caption
                    "clean_title": clean_title, # Store cleaned title for filename
                    "url": movie_url, # This is the original movie page URL
                    "source": "SkyMoviesHD"
                })
    except requests.exceptions.Timeout:
        logging.error(f"Timeout fetching SkyMoviesHD main page.")
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
        logging.info("Classic Cinema Bot started successfully!")


    async def download_thumbnail(self):
        path = Path("thumbnail.jpg")
        if path.exists():
            return path
        
        ssl_context = await create_ssl_context()
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(THUMBNAIL_URL, timeout=60) as response:
                    if response.status == 200:
                        with open(path, "wb") as f:
                            async for chunk in response.content.iter_chunked(1024):
                                f.write(chunk)
                        logging.info("Thumbnail downloaded successfully.")
                        return path
                    else:
                        logging.warning(f"Failed to download thumbnail: HTTP Status {response.status}")
                        return None
        except aiohttp.ClientError as e:
            logging.error(f"AIOHTTP client error during thumbnail download: {e}")
            return None
        except asyncio.TimeoutError:
            logging.error(f"Thumbnail download timed out.")
            return None
        except Exception as e:
            logging.error(f"Unexpected error during thumbnail download: {e}")
            return None

    async def scrape_and_process_loop(self):
        while True:
            try:
                archive_movies = await scrape_archive_org()
                skymovieshd_movies_meta = await scrape_skymovieshd_recent()

                all_movies_to_process = []
                for movie_meta in skymovieshd_movies_meta:
                    # Only process if the main movie page URL hasn't been completed
                    if not self.db.is_completed(movie_meta['url']):
                        download_links_from_page = await skymovieshd_scrape_movie_page(movie_meta['url'])
                        
                        if download_links_from_page:
                            # Create one entry with all links for SkyMoviesHD movies
                            all_movies_to_process.append({
                                "title": movie_meta['title'],      # Original full title
                                "clean_title": movie_meta['clean_title'], # Cleaned title for filename
                                "url": movie_meta['url'],          # Original movie page URL
                                "all_download_urls": download_links_from_page, # List of all direct links
                                "source": movie_meta['source']
                            })
                        else:
                            logging.info(f"No download links found for SkyMoviesHD movie: {movie_meta['title']} ({movie_meta['url']})")
                            await self.send_message(OWNER.ID, f"⚠️ No download links found for SkyMoviesHD movie: **{movie_meta['clean_title']}** ([Link]({movie_meta['url']}))", disable_web_page_preview=True)
                            # Mark the main movie page as completed even if no links found, to avoid reprocessing it
                            self.db.mark_completed(movie_meta['url'])

                # Add archive movies to the list. Archive movies already have 'download_url' and not 'all_download_urls'.
                # For consistency in process_movie, ensure 'all_download_urls' is present, even if empty/single.
                for movie_arc in archive_movies:
                    movie_arc['all_download_urls'] = [movie_arc['download_url']] # Make it a list for consistent iteration
                    movie_arc['clean_title'] = sanitize_filename(movie_arc['title']) # Add clean_title for consistent filename generation
                    all_movies_to_process.append(movie_arc)


                for movie_data in all_movies_to_process:
                    # process_movie will handle checking completion and marking
                    await self.process_movie(movie_data)
                    await asyncio.sleep(5) # Add a small delay between processing each movie to avoid rate limits

            except Exception as e:
                logging.error(f"Error in scrape loop: {e}", exc_info=True)
                await self.send_message(OWNER.ID, f"❌ Scrape Error: {str(e)}")

            logging.info("Scrape cycle complete. Waiting for next cycle.")
            await asyncio.sleep(3600) # Wait for an hour before the next scrape cycle

    async def process_movie(self, movie: Dict):
        source = movie['source']
        title = movie['title'] # Original full title
        clean_title = movie.get('clean_title', sanitize_filename(title)) # Cleaned title for filename
        original_page_url = movie['url'] # Original movie page URL for completion marking

        is_skymovieshd = (source == "SkyMoviesHD")
        
        # Check if the original page URL has already been processed and marked as completed
        # This prevents reprocessing a movie that was already sent or had its links shared
        if self.db.is_completed(original_page_url):
            logging.info(f"Skipping already completed movie: {title} from {source} (URL: {original_page_url})")
            return

        download_attempts_made = 0
        successful_download = False

        # Determine the list of URLs to try
        download_urls_to_try = movie.get('all_download_urls', [])
        if not download_urls_to_try and movie.get('download_url'): # Fallback for single download_url (e.g., Archive.org)
            download_urls_to_try = [movie['download_url']]

        if not download_urls_to_try:
            logging.info(f"No valid download URLs to try for {title} from {source}.")
            # Mark page as completed if no links were found or valid to avoid reprocessing
            self.db.mark_completed(original_page_url)
            await self.send_message(OWNER.ID, f"⚠️ No valid download links found or provided for **{title}** from {source}.")
            return

        for current_dl_url in download_urls_to_try:
            download_attempts_made += 1
            try:
                os.makedirs(DOWNLOAD_DIR, exist_ok=True)
                filename = f"{clean_title[:50]}{SUFFIX}{Path(current_dl_url).suffix or '.mp4'}"
                save_path = DOWNLOAD_DIR / filename

                await self.send_message(OWNER.ID, f"⬇️ Downloading: **{clean_title}** from {source} (Attempt {download_attempts_made}/{len(download_urls_to_try)})")
                success = await download_file(current_dl_url, save_path)
                
                if success:
                    file_size = save_path.stat().st_size / (1024 * 1024)
                    if file_size > MAX_FILE_SIZE_MB:
                        await self.send_message(OWNER.ID, f"❌ File too big ({file_size:.2f}MB): **{filename}**. Skipping this download link.")
                        save_path.unlink()
                        continue # Try next download link if available
                    if file_size < MIN_FILE_SIZE_MB:
                        await self.send_message(OWNER.ID, f"❌ File too small ({file_size:.2f}MB): **{filename}**. Skipping this download link.")
                        save_path.unlink()
                        continue # Try next download link if available

                    metadata = await get_movie_metadata(title, movie.get('keywords', ''), source)
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
                    # Mark the original movie page URL as completed since a file was successfully sent
                    self.db.mark_completed(original_page_url)
                    
                    await self.send_message(OWNER.ID, f"✅ Successfully sent: **{clean_title}**")
                    successful_download = True
                    return # Exit after first successful download and send

                else:
                    await self.send_message(OWNER.ID, f"❌ Failed to download from this link: `{current_dl_url}`. Trying next link if available.")
                    continue # Try next download link if available

            except Exception as e:
                await self.send_message(OWNER.ID, f"⚠️ Error with download link `{current_dl_url}` for **{clean_title}** from {source}: {str(e)}. Trying next link if available.")
                logging.error(f"Error processing download link {current_dl_url} for movie {clean_title}: {e}", exc_info=True)
                continue # Try next download link if available
        
        # If we reach this point, it means all download attempts for this movie failed
        if not successful_download:
            if is_skymovieshd and movie.get('all_download_urls'):
                quality_info = get_quality_info_from_title(title) # Use original title for quality
                
                gd_txt = f"🎬 **{clean_title}**\n" # Use clean title for main display
                gd_txt += f"<i>{quality_info}</i>\n\n"
                gd_txt += "<b>Download Links:</b>\n"
                for i, link in enumerate(movie['all_download_urls'], start=1):
                    gd_txt += f"{i}. `{link}`\n" # Use backticks for links to prevent issues with special chars
                
                await self.send_message(OWNER.ID, gd_txt, disable_web_page_preview=True)
                await self.send_message(OWNER.ID, f"⚠️ No direct file sent for **{clean_title}**. Shared download links instead.")
            else:
                # Fallback for Archive.org or if no all_download_urls were available (shouldn't happen for SkyMoviesHD now)
                await self.send_message(OWNER.ID, f"❌ All download attempts failed for **{clean_title}** from {source}. No direct file or alternative links shared.")
            
            # Mark the original movie page URL as completed even if all attempts failed or links were shared
            # This prevents re-processing this particular movie page in the future
            self.db.mark_completed(original_page_url)


# ------------------ Main ------------------
async def main():
    global db
    db = MongoDB()
    bot = ClassicCinemaBot()
    await bot.start()
    await idle()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    asyncio.run(main())
