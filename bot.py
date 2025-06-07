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
import libtorrent as lt
from config import BOT, API, OWNER

# ------------------ Constants ------------------
DOWNLOAD_DIR = Path(__file__).parent / "downloads"
THUMBNAIL_URL = "https://i.ibb.co/MDwd1f3D/6087047735061627461.jpg"
SUFFIX = " -@MNTGX.-"
MAX_FILE_SIZE_MB = 1900
MIN_FILE_SIZE_MB = 100
MONGODB_URI = "mongodb+srv://mntgx:mntgx@cluster0.pzcpq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Sources
ARCHIVE_ORG_FEEDS = [
    "https://archive.org/services/collection-rss.php?mediatype=movies"
]
SKYMOVIESHD_BASE_URL = "https://skymovieshd.dance"
TAMILBLASTERS_BASE_URL = "https://www.tamilblasters.co"  # Update if domain changes
YTS_RSS_URL = "https://yts.mx/rss/0/all/all/0"
EZTV_RSS_URL = "https://eztvx.to/ezrss.xml"

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
            async with session.get(url, timeout=300) as response:
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
    match = re.search(r'(\d+p\s*(?:HEVC|BluRay|HDRip|WEB-DL|DVDScr|DVDRip|BRRip|x265|x264|MP4|MKV)?.*?)', title, re.IGNORECASE)
    if match:
        extracted = match.group(1).strip()
        extracted = re.sub(r'\s*(ORG\.|Dual Audio|ESubs|x26[45]|AAC|DD5\.1|HDR|Esubs)\s*$', '', extracted, flags=re.IGNORECASE).strip()
        return extracted
    
    year_match = re.search(r'\(\d{4}\)\s*(.*)', title)
    if year_match:
        remaining_title = year_match.group(1).split('[')[0].strip()
        remaining_title = remaining_title.split('.')[0].strip()
        return remaining_title if remaining_title else "Multiple qualities"
    return "Multiple qualities"

async def get_movie_metadata(title: str, keywords: str = "", source: str = "Unknown") -> Dict:
    year_match = re.search(r'(19\d{2}|20\d{2})', title)
    years = extract_years_from_keywords(keywords)
    return {
        "title": title,
        "year": year_match.group(1) if year_match else "N/A",
        "years": years if years else "N/A",
        "source": source
    }

# ------------------ Torrent Download with libtorrent ------------------
async def download_torrent(url: str, title: str, save_path: Path) -> bool:
    """
    Downloads a torrent file or magnet link using libtorrent and monitors progress.
    Returns True if video file(s) are successfully downloaded, False otherwise.
    """
    try:
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        
        # Initialize libtorrent session
        ses = lt.session()
        ses.listen_on(6881, 6891)  # Default torrent port range
        
        # Handle magnet link or .torrent file
        if url.startswith("magnet:"):
            params = lt.parse_magnet_uri(url)
            params.save_path = str(DOWNLOAD_DIR)
            handle = ses.add_torrent(params)
        else:
            res = requests.get(url, allow_redirects=False, timeout=15)
            res.raise_for_status()
            if not res.content:
                logging.error(f"⚠️ Empty torrent file content for {title}")
                return False
            info = lt.torrent_info(lt.bencode(res.content))
            params = {"save_path": str(DOWNLOAD_DIR), "ti": info}
            handle = ses.add_torrent(params)
        
        logging.info(f"✅ Added torrent: {title}")
        
        # Monitor download progress
        while not handle.is_seed():
            s = handle.status()
            logging.info(f"⏳ Downloading {title}: {s.progress * 100:.2f}%")
            await asyncio.sleep(10)  # Check every uses seconds
        
        # Find video files in the download directory
        video_extensions = (".mp4", ".mkv", ".avi")
        torrent_info = handle.torrent_file()
        downloaded_files = []
        for file in torrent_info.files():
            if file.path.lower().endswith(video_extensions):
                file_path = DOWNLOAD_DIR / file.path
                if file_path.exists():
                    file_size = file_path.stat().st_size / (1024 * 1024)  # Size in MB
                    if file_size > MAX_FILE_SIZE_MB or file_size < MIN_FILE_SIZE_MB:
                        logging.info(f"❌ File size {file_size:.2f}MB out of range for {file.path}")
                        file_path.unlink(missing_ok=True)
                        continue
                    downloaded_files.append(file_path)
        
        if downloaded_files:
            # Rename first video file to match desired save_path
            if downloaded_files:
                first_file = downloaded_files[0]
                first_file.rename(save_path)
                logging.info(f"✅ Download complete: {title} - Renamed to {save_path}")
                # Clean up other files if any
                for extra_file in downloaded_files[1:]:
                    extra_file.unlink(missing_ok=True)
                return True
        else:
            logging.error(f"⚠️ No valid video files found for {title}")
            return False
    except Exception as e:
        logging.error(f"❌ Failed to download torrent {title}: {e}")
        return False
    finally:
        # Clean up session
        if 'handle' in locals():
            ses.remove_torrent(handle)
        if 'ses' in locals():
            del ses

# ------------------ Scrapers ------------------
async def scrape_archive_org() -> List[Dict]:
    movies = []
    for feed_url in ARCHIVE_ORG_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                if not db.is_completed(entry.link):
                    video_links = [
                        media["url"] for media in entry.get("media_content", [])
                        if media.get("type", "").startswith("video/") and "url" in media
                    ]
                    
                    if video_links:
                        movies.append({
                            "title": entry.title,
                            "url": entry.link,
                            "download_url": video_links[0],
                            "keywords": entry.get("media_keywords", ""),
                            "source": "Archive.org",
                            "all_download_urls": []
                        })
        except Exception as e:
            logging.error(f"Error scraping Archive.org feed {feed_url}: {e}")
    return movies

async def skymovieshd_scrape_movie_page(url: str) -> List[str]:
    """Scrapes a SkyMoviesHD movie page for direct download links and torrent/magnet links."""
    try:
        res = requests.get(url, allow_redirects=False, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        
        download_links = []
        _cache = []

        # Find direct links via howblogs.xyz
        for link in soup.select('a[href*="howblogs.xyz"]'):
            if link['href'] in _cache:
                continue
            _cache.append(link['href'])
            
            try:
                resp = requests.get(link['href'], allow_redirects=False, timeout=10)
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
        
        # Find torrent and magnet links
        for link in soup.select('a[href$=".torrent"], a[href^="magnet:"]'):
            if link['href'] in _cache:
                continue
            _cache.append(link['href'])
            download_links.append(link['href'])
        
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
        res = requests.get(SKYMOVIESHD_BASE_URL, allow_redirects=False, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        for a_tag in soup.select('div[class="Fmvideo"] > b > a[href*="/movie/"]'):
            movie_url = a_tag['href']
            if not movie_url.startswith('http'):
                movie_url = SKYMOVIESHD_BASE_URL + movie_url

            if not db.is_completed(movie_url):
                title = a_tag.text.strip()
                clean_title = re.sub(r'\[\d+MB\]', '', title).strip()

                movies.append({
                    "title": title,
                    "clean_title": clean_title,
                    "url": movie_url,
                    "source": "SkyMoviesHD"
                })
    except requests.exceptions.Timeout:
        logging.error(f"Timeout fetching SkyMoviesHD main page.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch or parse SkyMoviesHD main page: {e}")
    return movies

async def scrape_tamilblasters() -> List[Dict]:
    """Scrapes the TamilBlasters website for recent forum topic links."""
    movies = []
    try:
        res = requests.get(TAMILBLASTERS_BASE_URL, allow_redirects=False, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        
        post_links = [a["href"] for a in soup.select('a[href*="/forums/topic/"]') if a.get("href")]
        post_links = list(set(post_links))
        logging.info(f"Found {len(post_links)} thread links on TamilBlasters homepage.")
        
        for post_url in post_links[:15]:  # Limit to 15 posts
            if not db.is_completed(post_url):
                try:
                    full_url = post_url if post_url.startswith('http') else TAMILBLASTERS_BASE_URL + post_url
                    res = requests.get(full_url, allow_redirects=False, timeout=15)
                    res.raise_for_status()
                    post_soup = BeautifulSoup(res.text, 'html.parser')
                    
                    title_tag = post_soup.select_one("h1.thread-title")
                    title = title_tag.get_text(strip=True) if title_tag else "Untitled"
                    clean_title = sanitize_filename(title)
                    
                    download_links = []
                    _cache = []
                    # Find torrent and magnet links
                    for link in post_soup.select('a[href$=".torrent"], a[href^="magnet:"]'):
                        if link['href'] in _cache:
                            continue
                        _cache.append(link['href'])
                        download_links.append(link['href'])
                    
                    if download_links:
                        movies.append({
                            "title": title,
                            "clean_title": clean_title,
                            "url": full_url,
                            "all_download_urls": download_links,
                            "source": "TamilBlasters",
                            "keywords": ""  # No keywords in TamilBlasters
                        })
                    else:
                        logging.info(f"No download links found for TamilBlasters movie: {title} ({full_url})")
                except Exception as e:
                    logging.error(f"⚠️ TamilBlasters post error ({post_url}): {e}")
    except requests.exceptions.Timeout:
        logging.error(f"Timeout fetching TamilBlasters main page.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch or parse TamilBlasters main page: {e}")
    return movies

async def scrape_yts() -> List[Dict]:
    """Scrapes the YTS RSS feed for recent movie torrents."""
    movies = []
    try:
        feed = feedparser.parse(YTS_RSS_URL)
        for entry in feed.entries:
            if not db.is_completed(entry.link):
                title = entry.title
                summary = entry.get("summary", "")
                download_url = entry.enclosures[0]["href"] if entry.enclosures else entry.link
                movies.append({
                    "title": title,
                    "clean_title": sanitize_filename(title),
                    "url": entry.link,
                    "all_download_urls": [download_url],
                    "keywords": "",  # YTS RSS doesn't provide keywords
                    "source": "YTS"
                })
    except Exception as e:
        logging.error(f"Error scraping YTS RSS {YTS_RSS_URL}: {e}")
    return movies

async def scrape_eztv() -> List[Dict]:
    """Scrapes the EZTV RSS feed for recent movie/TV torrents."""
    movies = []
    try:
        feed = feedparser.parse(EZTV_RSS_URL)
        for entry in feed.entries:
            if not db.is_completed(entry.link):
                title = entry.title
                download_url = entry.get("torrent_magneturi", entry.link)
                movies.append({
                    "title": title,
                    "clean_title": sanitize_filename(title),
                    "url": entry.link,
                    "all_download_urls": [download_url],
                    "keywords": "",  # EZTV RSS doesn't provide keywords
                    "source": "EZTV"
                })
    except Exception as e:
        logging.error(f"Error scraping EZTV RSS {EZTV_RSS_URL}: {e}")
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
                skymovieshd_movies = await scrape_skymovieshd_recent()
                tamilblasters_movies = await scrape_tamilblasters()
                yts_movies = await scrape_yts()
                eztv_movies = await scrape_eztv()

                all_movies_to_process = []
                for movie_meta in skymovieshd_movies:
                    if not self.db.is_completed(movie_meta['url']):
                        download_links_from_page = await skymovieshd_scrape_movie_page(movie_meta['url'])
                        
                        if download_links_from_page:
                            all_movies_to_process.append({
                                "title": movie_meta['title'],
                                "clean_title": movie_meta['clean_title'],
                                "url": movie_meta['url'],
                                "all_download_urls": download_links_from_page,
                                "source": movie_meta['source']
                            })
                        else:
                            logging.info(f"No download links found for SkyMoviesHD movie: {movie_meta['title']} ({movie_meta['url']})")
                            await self.send_message(OWNER.ID, f"⚠️ No download links found for SkyMoviesHD movie: **{movie_meta['clean_title']}** ([Link]({movie_meta['url']}))", disable_web_page_preview=True)
                            self.db.mark_completed(movie_meta['url'])

                for movie in archive_movies:
                    movie['all_download_urls'] = [movie['download_url']]
                    movie['clean_title'] = sanitize_filename(movie['title'])
                    all_movies_to_process.append(movie)
                
                for movie in tamilblasters_movies:
                    all_movies_to_process.append(movie)
                
                for movie in yts_movies:
                    all_movies_to_process.append(movie)
                
                for movie in eztv_movies:
                    all_movies_to_process.append(movie)

                for movie_data in all_movies_to_process:
                    await self.process_movie(movie_data)
                    await asyncio.sleep(5)

            except Exception as e:
                logging.error(f"Error in scrape loop: {e}", exc_info=True)
                await self.send_message(OWNER.ID, f"❌ Scrape Error: {str(e)}")

            logging.info("Scrape cycle complete. Waiting for next cycle.")
            await asyncio.sleep(3600)

    async def process_movie(self, movie: Dict):
        source = movie['source']
        title = movie['title']
        clean_title = movie.get('clean_title', sanitize_filename(title))
        original_page_url = movie['url']
        is_skymovieshd = (source == "SkyMoviesHD")
        
        if self.db.is_completed(original_page_url):
            logging.info(f"Skipping already completed movie: {title} from {source} (URL: {original_page_url})")
            return

        download_attempts_made = 0
        successful_download = False

        download_urls_to_try = movie.get('all_download_urls', [])
        if not download_urls_to_try and movie.get('download_url'):
            download_urls_to_try = [movie['download_url']]

        if not download_urls_to_try:
            logging.info(f"No valid download URLs to try for {title} from {source}.")
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
                
                # Check if URL is a torrent or magnet link
                if current_dl_url.endswith('.torrent') or current_dl_url.startswith('magnet:'):
                    success = await download_torrent(current_dl_url, clean_title, save_path)
                else:
                    success = await download_file(current_dl_url, save_path)
                
                if success:
                    file_size = save_path.stat().st_size / (1024 * 1024)
                    if file_size > MAX_FILE_SIZE_MB:
                        await self.send_message(OWNER.ID, f"❌ File too big ({file_size:.2f}MB): **{filename}**. Skipping this download link.")
                        save_path.unlink(missing_ok=True)
                        continue
                    if file_size < MIN_FILE_SIZE_MB:
                        await self.send_message(OWNER.ID, f"❌ File too small ({file_size:.2f}MB): **{filename}**. Skipping this download link.")
                        save_path.unlink(missing_ok=True)
                        continue

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

                    save_path.unlink(missing_ok=True)
                    self.db.mark_completed(original_page_url)
                    
                    await self.send_message(OWNER.ID, f"✅ Successfully sent: **{clean_title}**")
                    successful_download = True
                    return

                else:
                    await self.send_message(OWNER.ID, f"❌ Failed to download from this link: `{current_dl_url}`. Trying next link if available.")
                    continue

            except Exception as e:
                await self.send_message(OWNER.ID, f"⚠️ Error with download link `{current_dl_url}` for **{clean_title}** from {source}: {str(e)}. Trying next link if available.")
                logging.error(f"Error processing download link {current_dl_url} for movie {clean_title}: {e}", exc_info=True)
                continue
        
        if not successful_download:
            if is_invalidymovieshd and movie.get('all_download_urls'):
                quality_info = get_quality_info_from_title(title)
                
                gd_txt = f"🎬 **{clean_title}**\n"
                gd_txt += f"<i>{quality_info}</i>\n\n"
                gd_txt += "<b>Download Links:</b>\n"
                for i, link in enumerate(movie['all_download_urls'], start=1):
                    gd_txt += f"{i}. `{link}`\n"
                
                await self.send_message(OWNER.ID, gd_txt, disable_web_page_preview=True)
                await self.send_message(OWNER.ID, f"⚠️ No direct file sent for **{clean_title}**. Shared download links instead.")
            else:
                await self.send_message(OWNER.ID, f"❌ All download attempts failed for **{clean_title}** from {source}. No direct file or alternative links shared.")
            
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
