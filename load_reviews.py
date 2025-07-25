import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from review_scraper import GoodreadsUserScraper
import os

# Config
DB_NAME = "goodreads"
DB_USER = "postgres"
DB_PASSWORD = "ZippyKiko88"
DB_HOST = "localhost"
DB_PORT = "5432"

def connect():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def create_tables(conn):
    with conn.cursor() as cur:
        # Interactions table (SVD-focused)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS interactions (
            user_id TEXT NOT NULL,
            book_id TEXT NOT NULL,
            rating INTEGER,
            timestamp DATE,
            PRIMARY KEY (user_id, book_id)
        );
        """)

        # Books metadata table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            book_id TEXT PRIMARY KEY,
            title TEXT,
            author TEXT,
            url TEXT
        );
        """)
        conn.commit()

def insert_interactions(conn, reviews, user_id):
    with conn.cursor() as cur:
        rows = []
        for r in reviews:
            date = None
            if r.date_read:
                try:
                    date = datetime.strptime(r.date_read, "%b %d, %Y").date()
                except:
                    pass
            if r.book_id:
                rows.append((
                    user_id, r.book_id, r.user_rating, date
                ))

        execute_values(cur, """
            INSERT INTO interactions (
                user_id, book_id, rating, timestamp
            ) VALUES %s
            ON CONFLICT (user_id, book_id) DO NOTHING;
        """, rows)
        conn.commit()

def insert_books(conn, reviews):
    with conn.cursor() as cur:
        rows = []
        seen_ids = set()
        for r in reviews:
            if r.book_id and r.book_id not in seen_ids:
                rows.append((r.book_id, r.book_title, r.book_author, r.book_url))
                seen_ids.add(r.book_id)

        execute_values(cur, """
            INSERT INTO books (
                book_id, title, author, url
            ) VALUES %s
            ON CONFLICT (book_id) DO NOTHING;
        """, rows)
        conn.commit()

if __name__ == "__main__":
    scraper = GoodreadsUserScraper()
    user_id = "101098244"
    reviews = scraper.scrape_user_reviews_by_user_id(user_id, max_pages=5)

    conn = connect()
    create_tables(conn)
    insert_interactions(conn, reviews, user_id)
    insert_books(conn, reviews)
    conn.close()

    print(f"Inserted {len(reviews)} reviews into `interactions` and `books` tables.")
