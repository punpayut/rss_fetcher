# worker.py (Complete & Corrected Final Version)

"""
FinanceFlow Worker (with Extended RSS Feeds & Rate Limit Fix)
- This script is designed to be run on a schedule (e.g., via GitHub Actions).
- It fetches news from a comprehensive list of English and Thai RSS feeds.
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
    'Yahoo Finance': 'https://finance.yahoo.com/news/rssindex'
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


# --- Worker-specific Service Classes ---
class NewsAggregator:
    def _fetch_from_feed(self, source_name: str, url: str) -> List[NewsItem]:
        logger.info(f"WORKER: Fetching from {source_name}")
        items = []
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:7]:
                try:
                    rss_summary = entry.get('summary', entry.get('description', ''))
                    cleaned_content = clean_html(rss_summary)
                    published_dt = datetime(*entry.published_parsed[:6]) if hasattr(entry, 'published_parsed') else datetime.now()
                    safe_id = url_to_firestore_id(entry.link)
                    
                    news_item = NewsItem(
                        id=safe_id,
                        title=entry.title,
                        link=entry.link,
                        source=source_name,
                        published=published_dt,
                        content=cleaned_content[:1500]
                    )
                    items.append(news_item)
                except Exception as e:
                    logger.warning(f"WORKER: Could not parse entry from {source_name} for link {entry.get('link', 'N/A')}: {e}")
        except Exception as e:
            logger.error(f"WORKER: Failed to parse feed {source_name} at {url}: {e}")
        return items

    def get_latest_news(self) -> List[NewsItem]:
        all_items = []
        with ThreadPoolExecutor(max_workers=len(RSS_FEEDS)) as executor:
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
        self.model = "llama3-8b-8192"

    def analyze_news_item(self, news_item: NewsItem) -> Optional[Dict[str, Any]]:
        if not self.client or not news_item.content:
            return None
        
        prompt = f"""
        You are a top-tier financial analyst AI for an app called FinanceFlow. Analyze the provided news summary from an RSS feed. The content might be in English or Thai.

        Source: {news_item.source}
        Title: {news_item.title}
        Provided Summary (Content): {news_item.content}

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
            logger.error(f"WORKER: Groq analysis failed for {news_item.link}. Error: {e}")
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
        # Fallback if Firestore is not available
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
            logger.error(f"An unexpected error occurred during AI analysis for {item.link}: {e}")

        if analysis:
            item.analysis = analysis
            data_to_save = asdict(item)
            data_to_save['processed_at'] = firestore.SERVER_TIMESTAMP
            data_to_save['published'] = item.published.isoformat()
            
            if analyzed_news_collection:
                analyzed_news_collection.document(item.id).set(data_to_save)
                saved_count += 1
                logger.info(f"-> Successfully saved analysis for article ID {item.id}")

        # Increased delay to 4 seconds to be more respectful of rate limits
        delay_seconds = 4
        logger.info(f"Waiting for {delay_seconds} seconds before next API call...")
        time.sleep(delay_seconds)
            
    logger.info(f"--- FinanceFlow Worker Finished: Successfully processed and saved {saved_count} of {len(items_to_process)} total new articles. ---")


if __name__ == "__main__":
    main()