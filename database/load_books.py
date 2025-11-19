import requests
from bs4 import BeautifulSoup
import time
import random
from supabase import create_client
import os
import logging
from dataclasses import dataclass
from typing import Optional, List
from dotenv import load_dotenv
import re

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Supabase
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

@dataclass
class BookMetadata:
    book_id: str
    title: Optional[str]
    description: Optional[str]
    author_id: Optional[str]
    author_name: Optional[str]
    average_rating: Optional[float]
    ratings_count: Optional[int]
    publication_year: Optional[int]
    genres: List[str]
    num_pages: Optional[int]
    cover_image_url: Optional[str]

class GoodreadsBookScraper:
    def __init__(self, delay_range=(1, 3)):
        self.session = requests.Session()
        self.delay_range = delay_range

        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def _rate_limit(self):
        time.sleep(random.uniform(*self.delay_range))

    def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        try:
            self._rate_limit()
            r = self.session.get(url, timeout=10)
            r.raise_for_status()
            return BeautifulSoup(r.content, "html.parser")
        except Exception as e:
            logger.error(f"Error fetching URL {url}: {e}")
            return None

    def scrape_book(self, book_id: str) -> Optional[BookMetadata]:
        url = f"https://www.goodreads.com/book/show/{book_id}"
        soup = self._fetch(url)
        if not soup:
            return None

        # --- Title ---
        title_elem = soup.find("h1", class_='Text Text__title1')
        title = title_elem.text.strip() if title_elem else None

        # --- Author ---
        author_elem = soup.find("a", class_="ContributorLink")
        author_name = author_elem.text.strip() if author_elem else None
        author_id = None
        if author_elem:
            match = re.search(r"/author/show/(\d+)", author_elem.get("href"))
            if match:
                author_id = match.group(1)

        # --- Description ---
        description = None
        desc_elem = soup.select_one("span.Formatted")

        if desc_elem:
            # Convert <br> tags to newlines
            for br in desc_elem.find_all("br"):
                br.replace_with("\n")

        # Extract all text including italic segments
        description = desc_elem.get_text(separator=" ", strip=True)

        # --- Ratings ---
        avg_elem = soup.find("div", class_='RatingStatistics__rating')
        average_rating = float(avg_elem.text.strip()) if avg_elem else None

        ratings_count = None
        meta = soup.find("div", class_="RatingStatistics__meta")

        if meta:
            count_span = meta.find("span")
            if count_span:
                text = count_span.get_text(strip=True)
                ratings_count = int(re.sub(r"\D", "", text)) if text else None

        # --- Publication year ---
        pub_elem = soup.find("p", attrs={'data-testid': 'publicationInfo'})
        publication_year = None
        if pub_elem:
            text = pub_elem.get_text()
            match = re.search(r"\d{4}", text)
            if match:
                publication_year = int(match.group(0))

        # --- Genres (shelves on left sidebar) ---
        genres = []

        for span in soup.select('[data-testid="genresList"] .Button__labelItem'):
            text = span.get_text(strip=True)
            if text and text != "...more":
                genres.append(text)

        # --- Num pages ---
        pages = None
        pages_elem = soup.find("p", attrs={"data-testid": "pagesFormat"})
        if pages_elem:
            match = re.search(r"(\d+)", pages_elem.text)
            if match:
                pages = int(match.group(1))

        # --- Cover image ---
        img_elem = soup.find("img", class_="ResponsiveImage", attrs={"role": "presentation"})
        cover_image_url = img_elem["src"] if img_elem else None


        return BookMetadata(
            book_id=book_id,
            title=title,
            description=description,
            author_id=author_id,
            author_name=author_name,
            average_rating=average_rating,
            ratings_count=ratings_count,
            publication_year=publication_year,
            genres=genres,
            num_pages=pages,
            cover_image_url=cover_image_url,
        )

    def save_book_to_supabase(self, book: BookMetadata):
        try:
            data = book.__dict__
            supabase.table("books").upsert(data).execute()
            logger.info(f"Saved book {book.book_id}")
        except Exception as e:
            logger.error(f"Failed to upsert book {book.book_id}: {e}")

if __name__ == "__main__":
    scraper = GoodreadsBookScraper()

    book_id = "44436221"  # Interior Chinatown
    book = scraper.scrape_book(book_id)

    if book:
        scraper.save_book_to_supabase(book)
        print("Done.")