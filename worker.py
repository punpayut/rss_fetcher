# worker.py (Final Version with ProcessPoolExecutor Fix)

"""
FinanceFlow Worker (with Extended RSS Feeds & Rate Limit Fix)
- This script is designed to be run on a schedule (e.g., via GitHub Actions).
- It fetches news from a comprehensive list of English and Thai RSS feeds.
- For select sources (Yahoo, CNBC, Investing.com), it follows the link to scrape the full article content.
  - For Investing.com, it uses a stealth headless browser (pyppeteer-stealth) to bypass advanced anti-bot measures.
- It analyzes news sequentially with a delay to respect API rate limits.
- It stores the structured data in Firestore.
"""

# --- Imports ---
import os
import json
import re
import base64
import time
from datetime import datetime
from typing import List, Dict, Optional, Any, Callable
### UPDATED ###
# We use ProcessPoolExecutor because asyncio/pyppeteer needs to run in a main thread.
from concurrent.futures import ProcessPoolExecutor 

import asyncio # Needed for pyppeteer

import feedparser
import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict

# Use pyppeteer and stealth plugin for the toughest websites
import pyppeteer
from pyppeteer_stealth import stealth

from groq import Groq
import logging
from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials, firestore

# --- Initialization ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Firebase Initialization for Worker ---
# NOTE: When using ProcessPoolExecutor, each process might need to initialize Firebase if not handled carefully.
# However, since we are only reading/writing data and not passing complex Firebase objects, 
# the main process initialization should suffice for credentials.

try:
    # Check if the app is already initialized to prevent errors in child processes.
    if not firebase_admin._apps:
        service_account_json_str = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
        if service_account_json_str:
            service_account_info = json.loads(service_account_json_str)
            cred = credentials.Certificate(service_account_info)
        else:
            cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)
    
    db_firestore = firestore.client()
    logger.info("WORKER: Firebase connection established.")
except Exception as e:
    logger.error(f"WORKER: Failed to initialize Firebase: {e}", exc_info=True)
    exit(1)

# --- Constants ---
RSS_FEEDS = {
    'Yahoo Finance': 'https://finance.yahoo.com/news/rssindex',
    'Investing Investment Ideas': 'https://www.investing.com/rss/news_1065.rss',
    'CNBC Top News': 'https://www.cnbc.com/id/100003114/device/rss/rss.html',
    'Stock Market News': 'https://www.investing.com/rss/news_25.rss',
    'MarketWatch': 'http://www.marketwatch.com/rss/topstories',
    'CNN Money': 'http://rss.cnn.com/rss/money_latest.rss',
    'Financial Times': 'https://www.ft.com/rss/home',
    'The Economist': 'https://www.economist.com/finance-and-economics/rss.xml',
    'Forex News': 'https://www.investing.com/rss/news_1.rss',
    'Commodities & Futures News': 'https://www.investing.com/rss/news_11.rss',
    'Economic Indicators': 'https://www.investing.com/rss/news_95.rss',
    'ประชาชาติธุรกิจ': 'https://www.prachachat.net/finance/feed'
}

# --- Data Classes & Utility Functions ---
@dataclass
class NewsItem:
    id: str
    title: str
    link: str
    source: str
    published: datetime
    content: str = ""
    analysis: Optional[Dict[str, Any]] = None

def clean_html(raw_html: str) -> str:
    if not raw_html: return ""
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext

def url_to_firestore_id(url: str) -> str:
    return base64.urlsafe_b64encode(url.encode('utf-8')).decode('utf-8')

# --- Scraper Functions ---
# NOTE: These functions must be defined at the top level of the module to be
# "picklable" by ProcessPoolExecutor.

def fetch_yahoo_article_content(url: str) -> str:
    """ Fetches and parses full article content from a Yahoo Finance news link using requests. """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        article_body = soup.find('div', class_='caas-body')
        if not article_body:
            article_body = soup.find('div', class_=re.compile(r'\bbody\b'))
        if article_body:
            return ' '.join(p.get_text(strip=True) for p in article_body.find_all('p'))
        # Using print for cross-process logging visibility
        print(f"WARNING: Could not find any known article body for Yahoo URL: {url}")
        return ""
    except Exception as e:
        print(f"ERROR: Error scraping Yahoo URL {url}: {e}")
        return ""

def fetch_cnbc_article_content(url: str) -> str:
    """ Fetches and parses full article content from a CNBC news link using requests. """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        article_body = soup.find('div', class_='ArticleBody-articleBody')
        if article_body:
            return ' '.join(p.get_text(strip=True) for p in article_body.find_all('p'))
        print(f"WARNING: Could not find article body for CNBC URL: {url}")
        return ""
    except Exception as e:
        print(f"ERROR: Error scraping CNBC URL {url}: {e}")
        return ""

async def fetch_investing_article_content_async(url: str) -> str:
    """ Fetches content from Investing.com using a stealth headless browser to bypass anti-bot. """
    browser = None
    content = ""
    base_url = 'https://www.investing.com'
    if url.startswith('/'):
        url = base_url + url
        
    try:
        executable_path = os.getenv('PYPPETEER_EXECUTABLE_PATH')
        browser_args = [
            '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas', '--no-first-run', '--no-zygote',
            '--single-process', '--disable-gpu'
        ]
        launch_options = {'headless': True, 'args': browser_args}
        if executable_path:
            launch_options['executablePath'] = executable_path
            
        browser = await pyppeteer.launch(launch_options)
        page = await browser.newPage()
        await stealth(page)

        print(f"INFO: [Stealth] Navigating to {url}...")
        await page.goto(url, {'waitUntil': 'networkidle0', 'timeout': 30000})

        article_body_html = await page.evaluate(
            '''() => {
                const body = document.querySelector('div#article_container') || document.querySelector('div#article');
                return body ? body.innerHTML : '';
            }'''
        )
        if article_body_html:
            soup = BeautifulSoup(article_body_html, 'html.parser')
            for element in soup.find_all("div", class_=["related_quotes", "js-related-article-wrapper"]):
                element.decompose()
            content = ' '.join(p.get_text(strip=True) for p in soup.find_all('p'))
        else:
            print(f"WARNING: Could not find article body after stealth navigation for Investing.com URL: {url}")
        
    except Exception as e:
        print(f"ERROR: An unexpected error occurred with pyppeteer for Investing.com URL {url}: {e}")
    finally:
        if browser:
            await browser.close()
    return content

def fetch_investing_article_content(url: str) -> str:
    return asyncio.run(fetch_investing_article_content_async(url))

SCRAPER_MAPPING: Dict[str, Callable[[str], str]] = {
    'Yahoo Finance': fetch_yahoo_article_content,
    'CNBC Top News': fetch_cnbc_article_content,
    'Investing Investment Ideas': fetch_investing_article_content,
    'Stock Market News': fetch_investing_article_content,
    'Forex News': fetch_investing_article_content,
    'Commodities & Futures News': fetch_investing_article_content,
    'Economic Indicators': fetch_investing_article_content,
}

# This function must be at the top level to be used by ProcessPoolExecutor
def fetch_and_process_feed(args):
    source_name, url = args
    # Using print for logging in child processes as the main logger can be tricky
    print(f"INFO: WORKER: Fetching from {source_name}")
    items = []
    try:
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        feed = feedparser.parse(url, agent=user_agent)

        for entry in feed.entries[:7]:
            try:
                scraped_content = ""
                
                if source_name in SCRAPER_MAPPING:
                    scraper_function = SCRAPER_MAPPING[source_name]
                    print(f"INFO: -> [{source_name}] Attempting to scrape full article...")
                    scraped_content = scraper_function(entry.link)

                if scraped_content:
                    content_to_analyze = scraped_content
                    print(f"INFO: -> [{source_name}] Scrape successful. Using full content.")
                else:
                    if source_name in SCRAPER_MAPPING:
                        print(f"WARNING: -> [{source_name}] Scrape failed. Falling back to RSS summary.")
                    content_to_analyze = entry.get('summary', entry.get('description', ''))
                    if not content_to_analyze:
                        print(f"WARNING: -> RSS summary is empty. Falling back to title.")
                        content_to_analyze = entry.get('title', '')
                
                cleaned_content = clean_html(content_to_analyze)
                if not cleaned_content:
                    print(f"WARNING: Skipping entry because no content could be found: {entry.link}")
                    continue

                published_dt = datetime(*entry.published_parsed[:6]) if hasattr(entry, 'published_parsed') else datetime.now()
                safe_id = url_to_firestore_id(entry.link)
                
                news_item = NewsItem(
                    id=safe_id, title=entry.title, link=entry.link, source=source_name,
                    published=published_dt, content=cleaned_content[:4000]
                )
                items.append(news_item)
            except Exception as e:
                print(f"WARNING: WORKER: Could not parse entry from {source_name} for link {entry.get('link', 'N/A')}: {e}")
    except Exception as e:
        print(f"ERROR: WORKER: A critical error occurred in _fetch_from_feed for {source_name}: {e}")
    return items

# --- Main Classes ---
class NewsAggregator:
    def get_latest_news(self) -> List[NewsItem]:
        all_items = []
        
        ### UPDATED ###
        # Use ProcessPoolExecutor to avoid asyncio/thread conflicts
        # It's more resource-heavy but robust for this use case.
        # Let's reduce workers to 2 to be safe on GitHub Actions resources.
        with ProcessPoolExecutor(max_workers=2) as executor:
            # map() is a convenient way to apply a function to a list of items
            results = executor.map(fetch_and_process_feed, RSS_FEEDS.items())
            for result_list in results:
                all_items.extend(result_list)
        
        unique_items_dict = {item.id: item for item in all_items if item.content}
        unique_items_list = list(unique_items_dict.values())
        sorted_items = sorted(unique_items_list, key=lambda x: x.published, reverse=True)
        logger.info(f"WORKER: Fetched and processed {len(sorted_items)} articles from RSS feeds.")
        return sorted_items

class AIProcessor:
    # ... (No change)
    def __init__(self):
        self.api_key = os.getenv("GROQ_API_KEY")
        self.client = Groq(api_key=self.api_key) if self.api_key else None
        if not self.client:
            logger.warning("WORKER: GROQ_API_KEY not found.")
        self.model = "llama3-70b-8192"
    def analyze_news_item(self, news_item: NewsItem) -> Optional[Dict[str, Any]]:
        # ... (No change)
        if not self.client or not news_item.content:
            return None
        prompt = f"""...""" # Prompt is unchanged
        try:
            chat_completion = self.client.chat.completions.create(messages=[{"role": "user", "content": prompt}], model=self.model, temperature=0.1, response_format={"type": "json_object"})
            return json.loads(chat_completion.choices[0].message.content)
        except Exception as e:
            logger.error(f"WORKER: Groq analysis failed for {news_item.link}. Error: {e}", exc_info=True)
            return None

# --- Main Execution Logic ---
def main():
    logger.info("--- Starting FinanceFlow Worker ---")
    
    aggregator = NewsAggregator()
    ai_processor = AIProcessor()

    latest_news = aggregator.get_latest_news()
    
    # The rest of main process is sequential, so it's fine.
    # ... (No change)
    items_to_process = []
    if db_firestore:
        # Check for existence of documents in the main process
        ids_to_check = [item.id for item in latest_news]
        existing_docs = analyzed_news_collection.where('id', 'in', ids_to_check).stream()
        existing_ids = {doc.id for doc in existing_docs}
        
        for item in latest_news:
            if item.id not in existing_ids:
                items_to_process.append(item)
    else:
        items_to_process = latest_news

    logger.info(f"WORKER: Found {len(items_to_process)} new articles to process.")
    if not items_to_process:
        logger.info("WORKER: No new articles to process. Exiting.")
        return

    logger.info(f"WORKER: Analyzing {len(items_to_process)} new articles one by one...")
    saved_count = 0
    
    for item in items_to_process:
        analysis = None
        try:
            logger.info(f"Analyzing: {item.title[:60]}...")
            analysis = ai_processor.analyze_news_item(item)
        except Exception as e:
            logger.error(f"An unexpected error occurred during AI analysis for {item.link}: {e}", exc_info=True)

        if analysis and db_firestore:
            item.analysis = analysis
            data_to_save = asdict(item)
            # Convert datetime back to string for saving, as it might not be JSON serializable
            # after being passed between processes.
            data_to_save['published'] = item.published.isoformat()
            data_to_save['processed_at'] = firestore.SERVER_TIMESTAMP
            
            analyzed_news_collection.document(item.id).set(data_to_save)
            saved_count += 1
            logger.info(f"-> Successfully saved analysis for article ID {item.id}")

        delay_seconds = 4
        logger.info(f"Waiting for {delay_seconds} seconds before next API call...")
        time.sleep(delay_seconds)
            
    logger.info(f"--- FinanceFlow Worker Finished: Successfully processed and saved {saved_count} of {len(items_to_process)} total new articles. ---")

if __name__ == "__main__":
    main()
