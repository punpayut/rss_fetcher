# worker.py (Version with Correct EOD HD API Parsing)

"""
FinanceFlow Worker (with Extended RSS Feeds, EOD HD API & Rate Limit Fix)
- This script is designed to be run on a schedule (e.g., once per hour via GitHub Actions).
- It fetches news from a comprehensive list of sources including RSS and EOD HD API.
- For EOD HD API, it correctly parses the response and respects the Free Plan limit.
- For select RSS sources (Yahoo, CNBC), it follows the link to scrape the full article content.
- It analyzes news sequentially and stores the structured data in Firestore.
"""

# --- Imports ---
import os
import json
import re
import base64
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any, Callable
from concurrent.futures import ThreadPoolExecutor
import http # Import http module for exception handling

import feedparser
import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict

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
try:
    service_account_json_str = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if service_account_json_str:
        service_account_info = json.loads(service_account_json_str)
        cred = credentials.Certificate(service_account_info)
    else:
        cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred)
    db_firestore = firestore.client()
    analyzed_news_collection = db_firestore.collection('analyzed_news')
    logger.info("WORKER: Firebase initialized successfully.")
except Exception as e:
    logger.error(f"WORKER: Failed to initialize Firebase: {e}", exc_info=True)
    exit(1)

# --- Constants ---
RSS_FEEDS = {
    'CNBC Top News': 'https://www.cnbc.com/id/100003114/device/rss/rss.html',
    'Yahoo Finance': 'https://finance.yahoo.com/news/rssindex',
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
# (ไม่มีการเปลี่ยนแปลงในส่วนนี้)
def fetch_yahoo_article_content(url: str) -> str:
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        article_body = soup.find('div', class_='caas-body')
        if not article_body:
            article_body = soup.find('div', class_=re.compile(r'\bbody\b'))
        if article_body:
            return ' '.join(p.get_text(strip=True) for p in article_body.find_all('p'))
        logger.warning(f"Could not find any known article body for Yahoo URL: {url}")
        return ""
    except Exception as e:
        logger.error(f"Error scraping Yahoo URL {url}: {e}", exc_info=False)
        return ""

def fetch_cnbc_article_content(url: str) -> str:
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        article_body = soup.find('div', class_='ArticleBody-articleBody')
        if article_body:
            return ' '.join(p.get_text(strip=True) for p in article_body.find_all('p'))
        logger.warning(f"Could not find article body for CNBC URL: {url}")
        return ""
    except Exception as e:
        logger.error(f"Error scraping CNBC URL {url}: {e}", exc_info=False)
        return ""

# --- Scraper Function Mapping ---
# (ไม่มีการเปลี่ยนแปลงในส่วนนี้)
SCRAPER_MAPPING: Dict[str, Callable[[str], str]] = {
    'Yahoo Finance': fetch_yahoo_article_content,
    'CNBC Top News': fetch_cnbc_article_content,
}


# --- Worker-specific Service Classes ---
class NewsAggregator:
    
    # --- METHOD ที่ถูกปรับปรุง ---
    def _fetch_from_eod_api(self) -> List[NewsItem]:
        """Fetches and parses news from the EOD HD API based on actual response structure."""
        api_token = os.getenv('EOD_HD_API_TOKEN')
        if not api_token:
            logger.info("WORKER: Skipping EOD HD API fetch because EOD_HD_API_TOKEN is not configured.")
            return []

        logger.info("WORKER: Fetching from EOD Historical Data API")
        items = []
        url = f"https://eodhd.com/api/news?api_token={api_token}&fmt=json&limit=10&language=en"

        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            api_data = response.json()

            for entry in api_data:
                try:
                    # ตรวจสอบว่ามีข้อมูลที่จำเป็นครบถ้วนหรือไม่ (link, content, date, title)
                    if not all(k in entry for k in ['link', 'content', 'date', 'title']):
                        logger.warning(f"WORKER: Skipping EOD HD entry due to missing key data. Title: {entry.get('title', 'N/A')}")
                        continue

                    # 1. สร้าง ID จาก link จริง เพื่อให้ไม่ซ้ำและเป็นมาตรฐานเดียวกับ RSS
                    safe_id = url_to_firestore_id(entry['link'])
                    
                    # 2. แปลง date string (ISO 8601) เป็น datetime object โดยตรง
                    published_dt = datetime.fromisoformat(entry['date'])

                    # 3. สร้าง NewsItem จากข้อมูลจริงที่ได้จาก API
                    news_item = NewsItem(
                        id=safe_id,
                        title=entry['title'],
                        link=entry['link'],
                        source="EOD News Data",
                        published=published_dt,
                        content=entry['content'][:4000] # จำกัดความยาว content
                    )
                    items.append(news_item)
                except Exception as e:
                    logger.warning(f"WORKER: Could not parse entry from EOD HD API for title '{entry.get('title', 'N/A')}': {e}", exc_info=False)
        
        except requests.exceptions.RequestException as e:
            logger.error(f"WORKER: Network error fetching EOD HD API: {e}", exc_info=False)
        except Exception as e:
            logger.error(f"WORKER: Critical error in _fetch_from_eod_api: {e}", exc_info=True)
        
        logger.info(f"WORKER: Fetched {len(items)} articles from EOD HD API.")
        return items

    def _fetch_from_feed(self, source_name: str, url: str) -> List[NewsItem]:
        # (ไม่มีการเปลี่ยนแปลงในส่วนนี้)
        logger.info(f"WORKER: Fetching from {source_name}")
        items = []
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            feed = feedparser.parse(response.content)

            if feed.bozo:
                logger.warning(f"WORKER: Feed from {source_name} might be malformed. Error: {feed.get('bozo_exception', 'Unknown')}")
            
            for entry in feed.entries[:3]:
                try:
                    content_to_analyze = ""
                    scraped_content = ""
                    
                    if source_name in SCRAPER_MAPPING:
                        if source_name == 'Yahoo Finance' and 'finance.yahoo.com' not in entry.link:
                             logger.info(f"-> [Yahoo Finance Partner] Skipping scrape for non-Yahoo domain: {entry.link[:70]}...")
                        else:
                            logger.info(f"-> [{source_name}] Attempting to scrape...")
                            scraper_function = SCRAPER_MAPPING[source_name]
                            scraped_content = scraper_function(entry.link)
                    
                    if scraped_content:
                        content_to_analyze = scraped_content
                    else:
                        if source_name in SCRAPER_MAPPING:
                            logger.warning(f"   -> [{source_name}] Scrape failed/skipped. Falling back to RSS summary.")
                        content_to_analyze = entry.get('summary', entry.get('description', ''))
                        if not content_to_analyze:
                            content_to_analyze = entry.get('title', '')
                    
                    cleaned_content = clean_html(content_to_analyze)
                    if not cleaned_content:
                        continue

                    published_dt = datetime.fromtimestamp(time.mktime(entry.published_parsed), tz=timezone.utc) if hasattr(entry, 'published_parsed') else datetime.now(timezone.utc)
                    
                    news_item = NewsItem(
                        id=url_to_firestore_id(entry.link),
                        title=entry.title,
                        link=entry.link,
                        source=source_name,
                        published=published_dt,
                        content=cleaned_content[:4000]
                    )
                    items.append(news_item)
                except Exception as e:
                    logger.warning(f"WORKER: Could not parse entry from {source_name} for link {entry.get('link', 'N/A')}: {e}", exc_info=False)
        
        except requests.exceptions.RequestException as e:
            logger.error(f"WORKER: Network error fetching {source_name}: {e}", exc_info=False)
        except Exception as e:
            logger.error(f"WORKER: Critical error in _fetch_from_feed for {source_name}: {e}", exc_info=True)
        return items

    def get_latest_news(self) -> List[NewsItem]:
        # (ไม่มีการเปลี่ยนแปลงในส่วนนี้)
        all_items = []
        eod_items = self._fetch_from_eod_api()
        all_items.extend(eod_items)
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            rss_futures = [executor.submit(self._fetch_from_feed, name, url) for name, url in RSS_FEEDS.items()]
            for future in rss_futures:
                all_items.extend(future.result())
        
        unique_items_dict = {item.id: item for item in all_items if item.content}
        unique_items_list = list(unique_items_dict.values())
        
        sorted_items = sorted(unique_items_list, key=lambda x: x.published, reverse=True)
        
        logger.info(f"WORKER: Fetched and processed {len(sorted_items)} articles from ALL sources.")
        return sorted_items

class AIProcessor:
    # (ไม่มีการเปลี่ยนแปลงในส่วนนี้)
    def __init__(self):
        self.api_key = os.getenv("GROQ_API_KEY")
        self.client = Groq(api_key=self.api_key) if self.api_key else None
        if not self.client:
            logger.warning("WORKER: GROQ_API_KEY not found.")
        self.model = "llama3-70b-8192"

    def analyze_news_item(self, news_item: NewsItem) -> Optional[Dict[str, Any]]:
        if not self.client or not news_item.content:
            return None
        prompt = f"""
        You are a top-tier financial analyst AI for an app called FinanceFlow. Analyze the provided news summary or full article content. The content might be in English or Thai.

        Source: {news_item.source}
        Title: {news_item.title}
        Provided Content: {news_item.content}

        Your primary task is to respond with a valid JSON object. This JSON must conform to the following structure:
        {{
          "summary_en": "A concise, one-paragraph summary of the article in English.",
          "summary_th": "A fluent, natural-sounding Thai translation of the English summary. If the original is already in Thai, make this summary a more concise version in Thai.",
          "sentiment": "Analyze the sentiment. Choose one: 'Positive', 'Negative', 'Neutral'.",
          "impact_score": "On a scale of 1-10, how impactful is this news for an average investor?",
          "affected_symbols": ["A list of stock ticker symbols (e.g., 'AAPL', 'NVDA') or Thai stock symbols (e.g., 'PTT', 'AOT') directly mentioned or heavily implied in the text."]
        }}

        Do not include any other text, explanations, or markdown. Your entire response must be only the JSON object itself.
        """
        try:
            chat_completion = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model=self.model,
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            return json.loads(chat_completion.choices[0].message.content)
        except Exception as e:
            logger.error(f"WORKER: Groq analysis failed for {news_item.link}. Error: {e}", exc_info=True)
            return None

def main():
    # (แนะนำให้ใช้การเช็คข้อมูลซ้ำแบบ chunking เพื่อประสิทธิภาพที่ดีกว่า)
    logger.info("--- Starting FinanceFlow Worker ---")
    
    aggregator = NewsAggregator()
    ai_processor = AIProcessor()

    latest_news = aggregator.get_latest_news()
    
    items_to_process = []
    if analyzed_news_collection:
        # ใช้ 'in' query เพื่อเช็คเอกสารทีละมากๆ (ไม่เกิน 30 ID ต่อครั้ง) จะเร็วกว่า
        all_ids = [item.id for item in latest_news]
        existing_ids = set()
        for i in range(0, len(all_ids), 30):
            chunk = all_ids[i:i+30]
            if chunk:
                docs = analyzed_news_collection.where('id', 'in', chunk).stream()
                for doc in docs:
                    existing_ids.add(doc.id)
        
        items_to_process = [item for item in latest_news if item.id not in existing_ids]
    else:
        items_to_process = latest_news

    logger.info(f"WORKER: Found {len(items_to_process)} new articles to process.")
    if not items_to_process:
        logger.info("WORKER: No new articles to process. Exiting.")
        return

    logger.info(f"WORKER: Analyzing {len(items_to_process)} new articles one by one...")
    saved_count = 0
    
    for item in items_to_process:
        analysis = ai_processor.analyze_news_item(item)

        if analysis:
            item.analysis = analysis
            data_to_save = asdict(item)
            data_to_save['published'] = item.published.isoformat()
            data_to_save['processed_at'] = firestore.SERVER_TIMESTAMP
            
            if analyzed_news_collection:
                analyzed_news_collection.document(item.id).set(data_to_save)
                saved_count += 1
                logger.info(f"-> Successfully saved analysis for article ID {item.id}")

        delay_seconds = 4 # หน่วงเวลาเหมือนเดิม
        logger.info(f"Waiting for {delay_seconds} seconds before next API call...")
        time.sleep(delay_seconds)
            
    logger.info(f"--- FinanceFlow Worker Finished: Successfully processed and saved {saved_count} of {len(items_to_process)} total new articles. ---")


if __name__ == "__main__":
    main()