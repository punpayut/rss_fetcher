# worker.py (Final Rate Limit Fix Version)

# --- Imports ---
import os, json, re, base64, time
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

try:
    service_account_json_str = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if service_account_json_str:
        cred = credentials.Certificate(json.loads(service_account_json_str))
    else:
        cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred)
    db_firestore = firestore.client()
    analyzed_news_collection = db_firestore.collection('analyzed_news')
    logger.info("WORKER: Firebase initialized successfully.")
except Exception as e:
    logger.error(f"WORKER: Failed to initialize Firebase: {e}", exc_info=True)
    exit(1)

# --- Constants, Data Classes, Utility Functions, Service Classes (Unchanged) ---
RSS_FEEDS = { ... }
@dataclass class NewsItem: ...
def clean_html(...): ...
def url_to_firestore_id(...): ...
class NewsAggregator: ...
class AIProcessor: ...
# (ส่วนนี้เหมือนเดิมจากคำตอบก่อนหน้าทุกประการ)


# --- Main Execution Logic (UPDATED) ---
def main():
    logger.info("--- Starting FinanceFlow Worker ---")
    
    aggregator = NewsAggregator()
    ai_processor = AIProcessor()

    latest_news = aggregator.get_latest_news()
    
    items_to_process = []
    if analyzed_news_collection:
        for item in latest_news:
            if not analyzed_news_collection.document(item.id).get().exists:
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