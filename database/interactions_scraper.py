import requests
from bs4 import BeautifulSoup
import time
import random
from supabase import create_client
import os
from urllib.parse import urljoin
import logging
from dataclasses import dataclass
from typing import List, Optional
import re
from datetime import datetime
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Supabase credentials
from dotenv import load_dotenv
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Missing Supabase credentials! Check your .env file.")
    raise EnvironmentError("SUPABASE_URL or SUPABASE_SERVICE_KEY not set.")


@dataclass
class Interaction:
    user_id: str
    book_id: str
    book_title: str
    book_author: str
    user_rating: Optional[int]
    date_read: Optional[str]
    shelves: List[str]
    book_url: Optional[str] = None


class GoodreadsUserScraper:
    def __init__(self, delay_range=(1, 3)):
        """
        Initialize the scraper with rate limiting
        
        Args:
            delay_range: Tuple of (min_delay, max_delay) in seconds between requests
        """
        self.session = requests.Session()
        self.delay_range = delay_range
        
        # Headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def _rate_limit(self):
        """Add random delay between requests"""
        delay = random.uniform(*self.delay_range)
        time.sleep(delay)
    
    def _make_request(self, url: str) -> Optional[BeautifulSoup]:
        """
        Make a request with error handling and rate limiting
        
        Args:
            url: URL to scrape
            
        Returns:
            BeautifulSoup object or None if request failed
        """
        try:
            self._rate_limit()
            logger.info(f"Requesting: {url}")
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            return BeautifulSoup(response.content, 'html.parser')
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None
    
    def get_user_profile_url(self, username: str) -> str:
        """Generate user profile URL from username"""
        return f"https://www.goodreads.com/user/show/{username}"
    
    def get_user_reviews_url(self, user_id: str, page: int = 1) -> str:
        """Generate user reviews URL"""
        return f"https://www.goodreads.com/review/list/{user_id}?page={page}&shelf=read&sort=date_read&order=d"
    
    def scrape_user_interactions(self, user_id: str, max_pages: int = None) -> List[Interaction]:
        self.current_user_id = user_id

        logger.info(f"Starting to scrape reviews for user ID: {user_id}")
        
        reviews = []
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                break
            
            reviews_url = self.get_user_reviews_url(user_id, page)
            soup = self._make_request(reviews_url)
            
            if not soup:
                logger.error(f"Failed to load page {page}")
                break
            
            review_rows = soup.find_all('tr', id=re.compile(r'review_\d+'))
            if not review_rows:
                logger.info(f"No more reviews found on page {page}")
                break
            
            logger.info(f"Processing page {page} with {len(review_rows)} reviews")
            
            page_reviews = []
            for row in review_rows:
                review = self.parse_review_row(row)
                if review:
                    page_reviews.append(review)
            
            reviews.extend(page_reviews)
            logger.info(f"Extracted {len(page_reviews)} reviews from page {page}")
            
            next_link = soup.find('a', class_='next_page')
            if not next_link:
                logger.info("Reached last page")
                break
            
            page += 1
        
        logger.info(f"Total reviews scraped: {len(reviews)}")
        return reviews

    def _normalize_date(self, date_str: Optional[str]) -> Optional[str]:
        """Convert Goodreads date formats into YYYY-MM-DD for Postgres"""
        if not date_str or date_str.lower() in {"not set", "none"}:
            return None
        
        date_str = date_str.strip()

        # Try a few common Goodreads formats
        formats = ["%b %d, %Y", "%b %Y", "%Y"]  # e.g., "Sep 25, 2025", "Sep 2025", "2025"
        for fmt in formats:
            try:
                parsed = datetime.strptime(date_str, fmt)
                return parsed.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        # If nothing works, return None
        return None

    
    def parse_review_row(self, row) -> Optional[Interaction]:
        """
        Parse a single review row from the reviews page
        
        Args:
            row: BeautifulSoup element representing a book review row
            
        Returns:
            BookReview object or None if parsing failed
        """
        try:
            # Extract book title and URL
            title_elem = row.find('td', class_='field title')
            if not title_elem:
                return None
                
            title_link = title_elem.find('a')
            book_title = title_link.get('title', '').strip() if title_link else ''
            book_url = urljoin('https://www.goodreads.com', title_link.get('href', '')) if title_link else None
            
            # Extract book ID from URL
            book_id = None
            if book_url:
                match = re.search(r'/book/show/(\d+)', book_url)
                if match:
                    book_id = match.group(1)
            
            # Extract author
            author_elem = row.find('td', class_='field author')
            book_author = author_elem.find('a').text.strip() if author_elem and author_elem.find('a') else ''

            # Extract user rating
            rating_elem = row.find('td', class_='field rating')
            user_rating = None
            if rating_elem:
                rating_div = rating_elem.find('div', class_='value')
                if rating_div:
                    stars_span = rating_div.find('span', class_='staticStars')
                    if stars_span and stars_span.has_attr('title'):
                        title = stars_span['title'].lower()
                        rating_map = {
                            "did not like it": 1,
                            "it was ok": 2,
                            "liked it": 3,
                            "really liked it": 4,
                            "it was amazing": 5
                        }
                        user_rating = rating_map.get(title)

            
            # Extract shelves
            shelves_elem = row.find('td', class_='field shelves')
            shelves = []

            if shelves_elem:
                shelf_links = shelves_elem.find_all('a', class_='shelfLink')

                shelves = [link.text.strip() for link in shelf_links]
            
            
            # Extract date read
            date_spans = row.find_all('span', class_='date_read_value')
            dates_read = []
            for span in date_spans:
                raw_date = span.text.strip()
                normalized = self._normalize_date(raw_date)
                if normalized:
                    dates_read.append(normalized)
            
            # Most recent date
            date_read = max(dates_read) if dates_read else None
            
            
            return Interaction(
                user_id=self.current_user_id,  # store the current user ID in the class
                book_title=book_title,
                book_author=book_author,
                user_rating=user_rating,
                date_read=date_read,
                book_url=book_url,
                book_id=book_id,
                shelves=shelves
            )
            
        except Exception as e:
            logger.error(f"Error parsing review row: {e}")
            return None

    def save_interactions_to_supabase(self, interactions: List[Interaction]):
        if not interactions:
            logger.warning("No interactions to save.")
            return

        data = []
        for inter in interactions:
            data.append({
                "user_id": inter.user_id,
                "book_id": inter.book_id,
                "user_rating": inter.user_rating,
                "date_read": inter.date_read,
                "shelf": 'read',
            })

        try:
            response = supabase.table("interactions").upsert(data).execute()
            logger.info(f"Saved {len(data)} interactions to Supabase.")
        except Exception as e:
            logger.error(f"Failed to save interactions to Supabase: {e}")



# Example usage
if __name__ == "__main__":
    scraper = GoodreadsUserScraper(delay_range=(2, 4))
    user_id = "101098244"

    try:
        interactions = scraper.scrape_user_interactions(user_id, max_pages=5)
        if interactions:
            scraper.save_interactions_to_supabase(interactions)
            print(f"✅ Saved {len(interactions)} interactions for {user_id}")
        else:
            print("No interactions found.")
    except KeyboardInterrupt:
        print("\nScraping interrupted.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
