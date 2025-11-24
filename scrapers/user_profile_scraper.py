# from database.supabase_client import supabase
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


# User Metadata Model
@dataclass
class UserMetadata:
    user_id: str
    join_date: Optional[str]
    last_active: Optional[str]
    location: Optional[str]
    num_ratings: Optional[int]
    avg_rating_given: Optional[float]

# Goodreads User Scraper
class GoodreadsUserProfileScraper:
    def __init__(self, delay_range=(1, 3)):
        self.delay_range = delay_range
        self.session = requests.Session()

        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.9",
        })

    # Rate Limiting
    def _rate_limit(self):
        time.sleep(random.uniform(*self.delay_range))

    # HTML Fetching Wrapper
    def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        try:
            self._rate_limit()
            r = self.session.get(url, timeout=10)
            r.raise_for_status()
            return BeautifulSoup(r.content, "html.parser")
        except Exception as e:
            logger.error(f"Error fetching URL {url}: {e}")
            return None

    # Scrape User Metadata (Profile Page Only)
    def scrape_user(self, user_id: str) -> Optional[UserMetadata]:
        url = f"https://www.goodreads.com/user/show/{user_id}"
        soup = self._fetch(url)
        if not soup:
            return None


        # -------- Join Date --------
        join_date = None
        last_active = None

        activity_title = soup.find("div", class_="infoBoxRowTitle", string="Activity")
        if activity_title:
            activity_value = activity_title.find_next("div", class_="infoBoxRowItem")
            if activity_value:
                activity_text = activity_value.get_text(strip=True)

                # Regex pattern
                pattern = r"Joined in ([A-Za-z]+\s+\d{4}),?\s*last active in ([A-Za-z]+\s+\d{4})"
                match = re.search(pattern, activity_text)

                if match:
                    join_date = match.group(1)
                    last_active = match.group(2) 

        # -------- Stats --------
        num_ratings = None
        avg_rating_given = None

        stats_elem = soup.find("div", class_="profilePageUserStatsInfo")
        if stats_elem:
            ratings_value = stats_elem.find_next("a")
            if ratings_value:
                ratings_text = ratings_value.get_text(strip=True)

                match = re.search(r"(\d+)", ratings_text)
                num_ratings = int(match.group(1))

            avg_ratings_value = ratings_value.find_next("a")
            if avg_ratings_value:
                avg_ratings_text = avg_ratings_value.get_text(strip=True)

                match = re.search(r"\(?(\d\.\d+)\)?", avg_ratings_text)
                avg_rating_given = float(match.group(1))

        # -------- Location --------
        location = None

        return UserMetadata(
            user_id = user_id,
            join_date = join_date,
            last_active = last_active,
            location = location,
            num_ratings = num_ratings,
            avg_rating_given = avg_rating_given,
        )

    # -------------------------------------------------------
    # Save to Supabase
    # -------------------------------------------------------
    def save_user_to_supabase(self, user: UserMetadata):
        try:
            supabase.table("users").upsert(user.__dict__).execute()
            logger.info(f"Saved user {user.user_id}")
        except Exception as e:
            logger.error(f"Failed to upsert user {user.user_id}: {e}")

# -----------------------------------------------------------
# Runner
# -----------------------------------------------------------
if __name__ == "__main__":
    scraper = GoodreadsUserProfileScraper()

    user_id = "90227573"  # example
    user = scraper.scrape_user(user_id)

    if user:
        scraper.save_user_to_supabase(user)
        print("Done.")
