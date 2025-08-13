import json
import psycopg2
from psycopg2.extras import execute_values

DB_NAME = "goodreads"
DB_USER = "postgres"
DB_PASSWORD = "ZippyKiko88"
DB_HOST = "localhost"
DB_PORT = "5432"

BOOKS_PATH = "data/goodreads_books.json"  # update if needed

def connect():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def load_books(path, batch_size=10000):
    batch = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            book = json.loads(line)
            avg_rating = safe_float(book.get("average_rating", 0))
            num_pages = book.get("num_pages")
            pub_year = book.get("publication_year")

            batch.append((
                book["book_id"],
                book.get("title"),
                avg_rating,
                int(num_pages) if str(num_pages).isdigit() else None,
                int(pub_year) if str(pub_year).isdigit() else None,
                book.get("image_url")
            ))

            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch  # final batch


def insert_books(conn, books):
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO book_metadata (
                book_id, title, average_rating, num_pages, publication_year, image_url
            ) VALUES %s
            ON CONFLICT (book_id) DO NOTHING;
        """, books)
        conn.commit()

if __name__ == "__main__":
    conn = connect()
    total = 0
    for i, batch in enumerate(load_books(BOOKS_PATH, batch_size=10000), 1):
        insert_books(conn, batch)
        total += len(batch)
        print(f"Inserted batch {i} ({total} total)")
    conn.close()
    print("All books inserted into book_metadata.")
