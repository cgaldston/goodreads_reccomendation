import requests
from bs4 import BeautifulSoup
import time
import random
import json
import csv
from urllib.parse import urljoin, urlparse
import logging
from dataclasses import dataclass
from typing import List, Dict, Optional
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class BookReview:
    """Data class to store book review information"""
    book_title: str
    book_author: str
    user_rating: Optional[int]
    review_text: Optional[str]
    date_read: Optional[str]
    book_url: Optional[str]
    book_id: Optional[str]
    shelves: List[str]

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
    
    def scrape_user_reviews_by_user_id(self, user_id: str, max_pages: int = None) -> List[BookReview]:
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

    
    def parse_review_row(self, row) -> Optional[BookReview]:
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
                shelf_links = shelves_elem.find_all('a')
                shelves = [link.text.strip() for link in shelf_links]
            
            # Extract date read
            date_elem = row.find('td', class_='field date_read')
            date_read = None
            if date_elem:
                date_div = date_elem.find('div', class_='value')
                if date_div:
                    date_read = date_div.text.strip()
            
            # Extract review text
            review_elem = row.find('td', class_='field review')
            review_text = None
            if review_elem:
                review_div = review_elem.find('div', class_='value')
                if review_div:
                    # Get text content, removing extra whitespace
                    review_text = ' '.join(review_div.get_text().split()).strip()
                    if not review_text:
                        review_text = None
            
            return BookReview(
                book_title=book_title,
                book_author=book_author,
                user_rating=user_rating,
                review_text=review_text,
                date_read=date_read,
                book_url=book_url,
                book_id=book_id,
                shelves=shelves
            )
            
        except Exception as e:
            logger.error(f"Error parsing review row: {e}")
            return None
    
    def scrape_user_reviews(self, username: str, max_pages: int = None) -> List[BookReview]:
        """
        Scrape all reviews for a given user
        
        Args:
            username: Goodreads username
            max_pages: Maximum number of pages to scrape (None for all)
            
        Returns:
            List of BookReview objects
        """
        logger.info(f"Starting to scrape reviews for user: {username}")
        
        # Get user ID
        user_id = self.extract_user_id(username)
        if not user_id:
            logger.error(f"Could not find user ID for username: {username}")
            return []
        
        logger.info(f"Found user ID: {user_id}")
        
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
            
            # Find review rows
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
            
            # Check if there's a next page
            next_link = soup.find('a', class_='next_page')
            if not next_link:
                logger.info("Reached last page")
                break
            
            page += 1
        
        logger.info(f"Total reviews scraped: {len(reviews)}")
        return reviews
    
    def save_reviews_to_csv(self, reviews: List[BookReview], filename: str):
        """Save reviews to CSV file"""
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['book_title', 'book_author', 'user_rating', 'review_text', 
                         'date_read', 'book_url', 'book_id', 'shelves']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for review in reviews:
                writer.writerow({
                    'book_title': review.book_title,
                    'book_author': review.book_author,
                    'user_rating': review.user_rating,
                    'review_text': review.review_text,
                    'date_read': review.date_read,
                    'book_url': review.book_url,
                    'book_id': review.book_id,
                    'shelves': '|'.join(review.shelves) if review.shelves else ''
                })
    
    def save_reviews_to_json(self, reviews: List[BookReview], filename: str):
        """Save reviews to JSON file"""
        reviews_data = []
        for review in reviews:
            reviews_data.append({
                'book_title': review.book_title,
                'book_author': review.book_author,
                'user_rating': review.user_rating,
                'review_text': review.review_text,
                'date_read': review.date_read,
                'book_url': review.book_url,
                'book_id': review.book_id,
                'shelves': review.shelves
            })
        
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(reviews_data, jsonfile, indent=2, ensure_ascii=False)

# Example usage
if __name__ == "__main__":
    scraper = GoodreadsUserScraper(delay_range=(2, 4))  # 2-4 second delays
    
    # Replace with actual username
    user_id = "101098244"  # Change this to test
    
    try:
        # Scrape reviews (limit to 5 pages for testing)
        reviews = scraper.scrape_user_reviews_by_user_id(user_id, max_pages=5)
        
        if reviews:
            # Save to files
            scraper.save_reviews_to_csv(reviews, f"{user_id}_reviews.csv")
            scraper.save_reviews_to_json(reviews, f"{user_id}_reviews.json")
            
            print(f"Successfully scraped {len(reviews)} reviews for {user_id}")
            
            # Print first few reviews as example
            for i, review in enumerate(reviews[:3]):
                print(f"\nReview {i+1}:")
                print(f"Title: {review.book_title}")
                print(f"Author: {review.book_author}")
                print(f"Rating: {review.user_rating}/5")
                print(f"Date Read: {review.date_read}")
                print(f"Review: {review.review_text[:200] if review.review_text else 'No review'}...")
        else:
            print("No reviews found or scraping failed")
            
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")