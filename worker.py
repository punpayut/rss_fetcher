# worker.py (The Ultimate Scraper Version)

"""
FinanceFlow Worker (with Extended RSS Feeds & Rate Limit Fix)
- Fetches news from various sources.
- For Yahoo Finance, it attempts to scrape full article content using multiple patterns. 
  If scraping fails, it gracefully falls back to using the article title, ensuring no news is skipped.
- Analyzes news and stores it in Firestore.
"""

# --- Imports (same as before) ---
import os
import json
import re
import base64
import time
from datetime import datetime
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor

import feedparser
import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict

from groq import Groq
import logging
from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials, firestore

# --- Initialization and other functions remain the same... ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Firebase Initialization (same as before) ---
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

RSS_FEEDS = {
    'Yahoo Finance': 'https://finance.yahoo.com/news/rssindex',
    'Investing.com': 'https://th.investing.com/rss/news_25.rss',
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

@dataclass
class NewsItem:
    id: str; title: str; link: str; source: str; published: datetime
    content: str = ""; analysis: Optional[Dict[str, Any]] = None

def clean_html(raw_html: str) -> str:
    if not raw_html: return ""; return re.sub(re.compile('<.*?>'), '', raw_html)

def url_to_firestore_id(url: str) -> str:
    return base64.urlsafe_b64encode(url.encode('utf-8')).decode('utf-8')


# --- THIS IS THE KEY UPDATED FUNCTION ---
def fetch_full_article_content(url: str) -> str:
    """
    Fetches and parses the full article content from a Yahoo Finance news link.
    It tries multiple patterns to find the article body.
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        article_body = None
        
        # Pattern 1: The most common article layout
        article_body = soup.find('div', class_='caas-body')
        
        # Pattern 2: Fallback for different layouts (like the one you found)
        if not article_body:
            logger.info(f"   -> 'caas-body' not found. Trying pattern 2 ('div.body')...")
            article_body = soup.find('div', class_=re.compile(r'\bbody\b')) # Use regex to find class 'body'

        if article_body:
            # Get all text from paragraph tags within the found body
            paragraphs = article_body.find_all('p')
            full_text = ' '.join(p.get_text(strip=True) for p in paragraphs)
            return full_text
        else:
            logger.warning(f"Could not find any known article body for URL: {url}")
            return ""
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch article from {url}: {e}")
        return ""
    except Exception as e:
        logger.error(f"An unexpected error occurred while scraping {url}: {e}", exc_info=True)
        return ""


# --- Worker-specific Service Classes ---
class NewsAggregator:
    # --- THIS FUNCTION IS ALSO UPDATED ---
    def _fetch_from_feed(self, source_name: str, url: str) -> List[NewsItem]:
        logger.info(f"WORKER: Fetching from {source_name}")
        items = []
        try:
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
            feed = feedparser.parse(url, agent=user_agent)

            for entry in feed.entries[:7]:
                try:
                    content_to_analyze = ""
                    
                    # --- SMART FALLBACK LOGIC ---
                    if source_name == 'Yahoo Finance':
                        logger.info(f"-> Attempting to scrape full article: {entry.title[:50]}...")
                        scraped_content = fetch_full_article_content(entry.link)
                        
                        if scraped_content:
                            logger.info(f"   -> Success! Scraped content found for: {entry.link}")
                            content_to_analyze = scraped_content
                        else:
                            # If scraping fails, fall back to using the title.
                            logger.warning(f"   -> Scraping failed. Falling back to using article title for: {entry.link}")
                            content_to_analyze = entry.get('title', '')
                    else:
                        # Fallback for all other feeds
                        content_to_analyze = entry.get('summary', entry.get('description', entry.get('title', '')))

                    cleaned_content = clean_html(content_to_analyze)
                    
                    if not cleaned_content:
                        logger.warning(f"Skipping entry because no content could be found: {entry.link}")
                        continue

                    published_dt = datetime(*entry.published_parsed[:6]) if hasattr(entry, 'published_parsed') else datetime.now()
                    safe_id = url_to_firestore_id(entry.link)
                    
                    news_item = NewsItem(
                        id=safe_id, title=entry.title, link=entry.link, source=source_name,
                        published=published_dt, content=cleaned_content[:4000] # Limit content length
                    )
                    items.append(news_item)
                except Exception as e:
                    logger.warning(f"WORKER: Could not parse entry from {source_name}: {entry.get('link', 'N/A')}", exc_info=True)
        except Exception as e:
            logger.error(f"WORKER: A critical error occurred in _fetch_from_feed for {source_name}", exc_info=True)
        return items

    # ... get_latest_news and other classes remain the same ...
    def get_latest_news(self) -> List[NewsItem]:
        all_items = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self._fetch_from_feed, name, url) for name, url in RSS_FEEDS.items()]
            for future in futures:
                all_items.extend(future.result())
        
        unique_items_dict = {item.id: item for item in all_items if item.content}
        unique_items_list = list(unique_items_dict.values())
        
        sorted_items = sorted(unique_items_list, key=lambda x: x.published, reverse=True)
        
        logger.info(f"WORKER: Fetched and processed {len(sorted_items)} articles from RSS feeds.")
        return sorted_items

class AIProcessor:
    # ... (No changes here)
    pass

def main():
    # ... (No changes here)
    pass


if __name__ == "__main__":
    main()