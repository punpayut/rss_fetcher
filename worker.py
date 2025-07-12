# worker.py (Final Version with Multi-Pattern Scraper & Smart Fallback)

"""
FinanceFlow Worker (with Extended RSS Feeds & Rate Limit Fix)
- This script is designed to be run on a schedule (e.g., via GitHub Actions).
- It fetches news from a comprehensive list of English and Thai RSS feeds.
- For Yahoo Finance, it follows the link to scrape the full article content for better AI analysis.
  If scraping fails (due to different page layouts), it gracefully falls back to using the title.
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
        
        # --- Multi-pattern search logic ---
        # Pattern 1: The most common article layout ('caas-body')
        article_body = soup.find('div', class_='caas-body')
        
        # Pattern 2: Fallback for different layouts (e.g., 'div.body.yf-1ir6o1g')
        if not article_body:
            logger.info(f"   -> 'caas-body' not found. Trying pattern 2 ('div.body')...")
            article_body = soup.find('div', class_=re.compile(r'\bbody\b'))

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
                        # Fallback logic for all other feeds
                        content_to_analyze = entry.get('summary', entry.get('description', entry.get('title', '')))

                    cleaned_content = clean_html(content_to_analyze)
                    
                    if not cleaned_content:
                        logger.warning(f"Skipping entry because no content could be found: {entry.link}")
                        continue

                    published_dt = datetime(*entry.published_parsed[:6]) if hasattr(entry, 'published_parsed') else datetime.now()
                    safe_id = url_to_firestore_id(entry.link)
                    
                    news_item = NewsItem(
                        id=safe_id,
                        title=entry.title,
                        link=entry.link,
                        source=source_name,
                        published=published_dt,
                        content=cleaned_content[:4000] # Increased limit for full articles
                    )
                    items.append(news_item)
                except Exception as e:
                    logger.warning(f"WORKER: Could not parse entry from {source_name} for link {entry.get('link', 'N/A')}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"WORKER: A critical error occurred in _fetch_from_feed for {source_name}", exc_info=True)
        return items

    def get_latest_news(self) -> List[NewsItem]:
        all_items = []
        with ThreadPoolExecutor(max_workers=5) as executor: # Reduced workers to be gentler when scraping
            futures = [executor.submit(self._fetch_from_feed, name, url) for name, url in RSS_FEEDS.items()]
            for future in futures:
                all_items.extend(future.result())
        
        unique_items_dict = {item.id: item for item in all_items if item.content}
        unique_items_list = list(unique_items_dict.values())
        
        sorted_items = sorted(unique_items_list, key=lambda x: x.published, reverse=True)
        
        logger.info(f"WORKER: Fetched and processed {len(sorted_items)} articles from RSS feeds.")
        return sorted_items

class AIProcessor:
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


# --- Main Execution Logic ---
def main():
    logger.info("--- Starting FinanceFlow Worker ---")
    
    aggregator = NewsAggregator()
    ai_processor = AIProcessor()

    latest_news = aggregator.get_latest_news()
    
    items_to_process = []
    if analyzed_news_collection:
        for item in latest_news:
            doc_ref = analyzed_news_collection.document(item.id)
            if not doc_ref.get().exists:
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

        if analysis:
            item.analysis = analysis
            data_to_save = asdict(item)
            data_to_save['processed_at'] = firestore.SERVER_TIMESTAMP
            data_to_save['published'] = item.published.isoformat()
            
            if analyzed_news_collection:
                analyzed_news_collection.document(item.id).set(data_to_save)
                saved_count += 1
                logger.info(f"-> Successfully saved analysis for article ID {item.id}")

        # Increased delay to be more respectful of API rate limits
        delay_seconds = 4
        logger.info(f"Waiting for {delay_seconds} seconds before next API call...")
        time.sleep(delay_seconds)
            
    logger.info(f"--- FinanceFlow Worker Finished: Successfully processed and saved {saved_count} of {len(items_to_process)} total new articles. ---")


if __name__ == "__main__":
    main()