import os
import csv
import psycopg2
from psycopg2.extras import execute_values

DB_NAME = "goodreads"
DB_USER = "postgres"
DB_PASSWORD = "ZippyKiko88"
DB_HOST = "localhost"
DB_PORT = "5432"

INTERACTIONS_PATH = "../data/goodreads_interactions.csv"
BOOK_MAP_PATH = "../data/book_id_map.csv"
BATCH_SIZE = 50_000

def connect():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        host=DB_HOST, port=DB_PORT
    )

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS interactions (
          user_id   TEXT NOT NULL,
          book_id   TEXT NOT NULL REFERENCES book_metadata(book_id),
          rating    FLOAT NOT NULL,
          timestamp TIMESTAMP,
          PRIMARY KEY (user_id, book_id)
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_interactions_user ON interactions(user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_interactions_book ON interactions(book_id);")
    conn.commit()

def load_book_map(path) -> dict:
    """
    Expect headers like: book_id_anonym,book_id
    """
    m = {}
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        # try common header spellings
        for r in rdr:
            k = r.get("book_id_csv")
            v = r.get("book_id")
            if k and v:
                m[k.strip()] = v.strip()
    return m

def get_valid_book_ids(conn):
    """Return a set of all book_ids in book_metadata (to satisfy FK)."""
    with conn.cursor() as cur:
        cur.execute("SELECT book_id FROM book_metadata;")
        rows = cur.fetchall()
    return {r[0] for r in rows}

UPSERT_SQL = """
INSERT INTO interactions (user_id, book_id, rating, timestamp)
VALUES %s
ON CONFLICT (user_id, book_id)
DO UPDATE SET rating = EXCLUDED.rating;  -- keep latest numeric rating
"""

def insert_batch(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows)
    conn.commit()

def main():
    if not os.path.exists(INTERACTIONS_PATH):
        raise FileNotFoundError(f"Missing interactions file: {INTERACTIONS_PATH}")
    if not os.path.exists(BOOK_MAP_PATH):
        raise FileNotFoundError(f"Missing book map: {BOOK_MAP_PATH}")

    conn = connect()
    ensure_schema(conn)

    print("Loading book_id map…")
    bmap = load_book_map(BOOK_MAP_PATH)
    print(f"Book map entries: {len(bmap):,}")

    print("Fetching valid book_ids from book_metadata…")
    valid_books = get_valid_book_ids(conn)
    print(f"Metadata book_ids: {len(valid_books):,}")

    total = 0
    batch = []

    with open(INTERACTIONS_PATH, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        # expected headers: user_id, book_id (anon), is_read, rating, ...
        for r in rdr:
            try:
                if (r.get("is_read") or "").strip() != "1":
                    continue
                rating_str = (r.get("rating") or "").strip()
                if rating_str == "":
                    continue
                rating = float(rating_str)
                if rating <= 0:
                    continue

                user_id = (r.get("user_id") or "").strip()          # keep anon user ids
                book_id_anon = (r.get("book_id") or "").strip()
                if not user_id or not book_id_anon:
                    continue

                real_book_id = bmap.get(book_id_anon)
                if not real_book_id or real_book_id not in valid_books:
                    continue  # avoid FK violations

                # clamp defensively
                if rating < 1: rating = 1.0
                if rating > 5: rating = 5.0

                batch.append((user_id, real_book_id, rating, None))

                if len(batch) >= BATCH_SIZE:
                    insert_batch(conn, batch)
                    total += len(batch)
                    print(f"Inserted +{len(batch)} (total {total:,})")
                    batch = []
            except Exception:
                # skip malformed lines; add logging if you want
                continue

    if batch:
        insert_batch(conn, batch)
        total += len(batch)
        print(f"Inserted +{len(batch)} (total {total:,})")

    conn.close()
    print(f"Done. Inserted {total:,} explicit interactions.")

if __name__ == "__main__":
    main()

