# test_feed.py
import feedparser
import logging

# ตั้งค่า logging ให้แสดงผลทุกอย่าง
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("--- Starting Minimal Feed Test ---")

url = 'https://finance.yahoo.com/news/rssindex'
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'

try:
    logger.info(f"Attempting to parse URL: {url} with User-Agent.")
    feed = feedparser.parse(url, agent=user_agent)

    logger.info("--- Feedparser Result ---")
    logger.info(f"Feed parsed. Number of entries: {len(feed.entries)}")
    logger.info(f"Feed Status: {feed.get('status', 'N/A')}")
    logger.info(f"Is feed malformed (bozo)? {feed.bozo}")
    if feed.bozo:
        logger.warning(f"Bozo Exception: {feed.get('bozo_exception', 'N/A')}")

    if feed.entries:
        logger.info("First entry found:")
        logger.info(f"  Title: {feed.entries[0].title}")
        logger.info(f"  Link: {feed.entries[0].link}")
    else:
        logger.warning("No entries were found in the feed.")

except Exception as e:
    logger.error("A critical exception occurred during the test.", exc_info=True)

logger.info("--- Minimal Feed Test Finished ---")