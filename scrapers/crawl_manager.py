import time
import random
import logging
import json
import os
from collections import deque
from database.supabase_client import supabase
from scrapers.user_profile_scraper import GoodreadsUserProfileScraper
from scrapers.reviewer_scraper import GoodreadsReviewerScraper
from scrapers.book_scraper import GoodreadsBookScraper
from scrapers.user_interactions_scraper import GoodreadsUserInteractionsScraper


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Define paths to visited files
CRAWL_DIR = "crawls"
VISITED_USERS_FILE = os.path.join(CRAWL_DIR, "visited_users.json")
VISITED_BOOKS_FILE = os.path.join(CRAWL_DIR, "visited_books.json")

class CrawlManager:

    def __init__(self, max_depth=1, reviewers_per_book=3):
        self.max_depth = max_depth
        self.reviewers_per_book = reviewers_per_book

        self.user_profile_scraper = GoodreadsUserProfileScraper()
        self.reviewer_scraper = GoodreadsReviewerScraper()
        self.book_scraper = GoodreadsBookScraper()
        self.user_interactions_scraper = GoodreadsUserInteractionsScraper()

        self.current_level_queue = deque()

        # --- LOAD STATE FROM JSON ---
        self.visited_users = self._load_json_set(VISITED_USERS_FILE)
        self.visited_books = self._load_json_set(VISITED_BOOKS_FILE)
        
        logger.info(f"Resuming crawl with {len(self.visited_users)} users and {len(self.visited_books)} books visited.")

    def _load_json_set(self, filepath):
        """Helper to load a JSON list and convert it to a set."""
        if not os.path.exists(filepath):
            return set()
        
        try:
            with open(filepath, 'r') as f:
                data = f.read()
                if not data: # Handle empty file
                    return set()
                return set(json.loads(data))
        except (json.JSONDecodeError, IOError):
            logger.warning(f"Could not read {filepath}, starting fresh.")
            return set()

    def _save_state(self):
        """Helper to save sets as JSON lists."""
        # Ensure directory exists
        os.makedirs(CRAWL_DIR, exist_ok=True)

        try:
            # Convert set to list for JSON serialization
            with open(VISITED_USERS_FILE, 'w') as f:
                json.dump(list(self.visited_users), f)
            
            with open(VISITED_BOOKS_FILE, 'w') as f:
                json.dump(list(self.visited_books), f)
                
        except IOError as e:
            logger.error(f"Failed to save state: {e}")

    def add_seed_user(self, user_id: str):
        """Load seed user to start crawl"""
        if user_id not in self.visited_users:
            self.current_level_queue.append(user_id)

    def run(self):
        current_depth = 0

        while current_depth < self.max_depth and self.current_level_queue:
            logger.info(f"---- Processing Depth {current_depth} | Queue: {len(self.current_level_queue)} ----")
            
            next_level_queue = deque()

            while self.current_level_queue:
                current_user = self.current_level_queue.popleft()

                if current_user in self.visited_users:
                    continue

                # Process the user
                self.process_user(current_user, next_level_queue)
                
                # Add to visited and SAVE immediately so we don't lose progress
                self.visited_users.add(current_user)
                self._save_state()

            self.current_level_queue = next_level_queue
            current_depth += 1

        logger.info("Crawl finished!")

    def process_user(self, user_id, next_level_queue):
        logger.info(f"Crawling user: {user_id}")

        try:
            # 1. Scrape Profile
            user_profile = self.user_profile_scraper.scrape_user(user_id)
            if user_profile:
                self.user_profile_scraper.save_user_to_supabase(user_profile)

            # 2. Scrape Interactions
            interactions = self.user_interactions_scraper.scrape_user_interactions(user_id)
            if not interactions:
                return

            self.user_interactions_scraper.save_interactions_to_supabase(interactions)

            # 3. Process Books & Find Reviewers
            for inter in interactions:
                book_id = inter.book_id
                
                # CHECK JSON HISTORY: Have we seen this book?
                if book_id not in self.visited_books:
                    book_meta = self.book_scraper.scrape_book(book_id)
                    if book_meta:
                        self.book_scraper.save_book_to_supabase(book_meta)
                    
                    self.visited_books.add(book_id)
                    # Save state here too if you want to be very safe about books
                    self._save_state() 
                    
                    time.sleep(random.uniform(1, 2))

                # Find Reviewers for next hop
                reviewers = self.reviewer_scraper.scrape_reviewers_for_book(book_id, limit=self.reviewers_per_book)
                for reviewer_id in reviewers:
                    if reviewer_id not in self.visited_users:
                        next_level_queue.append(reviewer_id)

        except Exception as e:
            logger.error(f"Error processing user {user_id}: {e}")

if __name__ == "__main__":
    manager = CrawlManager(max_depth=3, reviewers_per_book=5)

    # Seed user
    manager.add_seed_user("172940526")

    manager.run()