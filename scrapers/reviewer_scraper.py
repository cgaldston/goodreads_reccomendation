import requests
from bs4 import BeautifulSoup
import time
import random
import logging
import re

logger = logging.getLogger(__name__)


class GoodreadsReviewerScraper:
    """
    Scrapes reviewer user IDs from a Goodreads book page.
    Used for graph expansion (user -> book -> reviewer -> new users).
    """

    def __init__(self, delay_range=(1, 3)):
        self.session = requests.Session()
        self.delay_range = delay_range

        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def _rate_limit(self):
        time.sleep(random.uniform(*self.delay_range))

    def _fetch(self, url: str):
        try:
            self._rate_limit()
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def get_book_reviews_url(self, book_id: str, page=1) -> str:
        return f"https://www.goodreads.com/book/show/{book_id}?page={page}#other_reviews"

    def scrape_reviewers_for_book(self, book_id: str, limit=10) -> list:
        """
        Extracts user IDs of reviewers from the first review page of a book.

        Args:
            book_id: Goodreads book ID
            limit: Maximum number of users to return
        Returns:
            List of user_id strings
        """
        url = self.get_book_reviews_url(book_id)
        soup = self._fetch(url)
        if not soup:
            return []

        reviewers = []

        # Reviews are inside <div class="ReviewCard__user">
        review_cards = soup.find_all("div", class_="ReviewerProfile__name")
        # Alternative selector (older layout)

        if review_cards:
            for rc in review_cards:
                try:
                    # Extract user link
                    user_link = rc.find("a")
                    if not user_link or not user_link.get("href"):
                        continue

                    href = user_link.get("href")

                    # Extract user_id from /user/show/<id>
                    match = re.search(r"/user/show/(\d+)", href)
                    if match:
                        user_id = match.group(1)
                        reviewers.append(user_id)
                except Exception as e:
                    logger.error(f"Error parsing reviewer card: {e}")
                    continue

                if len(reviewers) >= limit:
                    break

        # Remove duplicates
        reviewers = list(dict.fromkeys(reviewers))

        logger.info(f"Found {len(reviewers)} reviewers for book {book_id}")

        return reviewers

if __name__ == "__main__":
    scraper = GoodreadsReviewerScraper()
    book_id = "44436221"  # Interior Chinatown
    reviewers = scraper.scrape_reviewers_for_book(book_id, limit=10)
    print("Reviewers:", reviewers)
