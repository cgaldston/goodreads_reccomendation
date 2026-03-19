"""ETL script: enrich book stubs with metadata from goodreads_books_*.json.

The interactions ETL already loaded stub rows (just book_id) for every book
with >= min_book_ratings interactions. This script fills in the remaining
columns for those stubs by streaming through the books JSONL file and upserting
only the rows whose book_id is already present in the books table.

Note: author_name is left NULL — the books JSON contains only author_id.
      genres is left NULL — populated separately from the genre file.

Usage:
    python -m src.etl.load_book_metadata
    python -m src.etl.load_book_metadata --file data/goodreads_books_history_biography.json
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from psycopg2.extras import execute_values
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from database.db import engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_FILE = Path("data/goodreads_books_history_biography.json")
DEFAULT_BATCH_SIZE = 2000
TOP_SHELVES_N = 10

_UPSERT_SQL = """
    INSERT INTO books (
        book_id, title, description, author_id,
        average_rating, ratings_count, publication_year,
        top_shelves, num_pages, cover_image_url, isbn,
        updated_at
    )
    VALUES %s
    ON CONFLICT (book_id) DO UPDATE SET
        title            = EXCLUDED.title,
        description      = EXCLUDED.description,
        author_id        = EXCLUDED.author_id,
        average_rating   = EXCLUDED.average_rating,
        ratings_count    = EXCLUDED.ratings_count,
        publication_year = EXCLUDED.publication_year,
        top_shelves      = EXCLUDED.top_shelves,
        num_pages        = EXCLUDED.num_pages,
        cover_image_url  = EXCLUDED.cover_image_url,
        isbn             = EXCLUDED.isbn,
        updated_at       = EXCLUDED.updated_at
"""

_COLS = [
    "book_id", "title", "description", "author_id",
    "average_rating", "ratings_count", "publication_year",
    "top_shelves", "num_pages", "cover_image_url", "isbn",
    "updated_at",
]


def _to_int(val: str) -> Optional[int]:
    """Cast a string to int; return None on empty or invalid input."""
    try:
        return int(val) if val else None
    except (ValueError, TypeError):
        return None


def _to_float(val: str) -> Optional[float]:
    """Cast a string to float; return None on empty or invalid input."""
    try:
        return float(val) if val else None
    except (ValueError, TypeError):
        return None


def _fetch_known_book_ids() -> set[str]:
    """Return the set of book_ids already loaded (stubs from interactions ETL)."""
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT book_id FROM books")
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def _parse_record(raw: dict) -> dict:
    """Extract and clean metadata fields from a raw books JSON record."""
    authors = raw.get("authors") or []
    author_id = authors[0]["author_id"] if authors else None

    shelves = raw.get("popular_shelves") or []
    top_shelves = [s["name"] for s in shelves[:TOP_SHELVES_N]]

    isbn = raw.get("isbn") or None

    return {
        "book_id": raw["book_id"],
        "title": raw.get("title") or None,
        "description": raw.get("description") or None,
        "author_id": author_id,
        "average_rating": _to_float(raw.get("average_rating", "")),
        "ratings_count": _to_int(raw.get("ratings_count", "")),
        "publication_year": _to_int(raw.get("publication_year", "")),
        "top_shelves": top_shelves or None,
        "num_pages": _to_int(raw.get("num_pages", "")),
        "cover_image_url": raw.get("image_url") or None,
        "isbn": isbn,
        "updated_at": datetime.now(timezone.utc),
    }


def _upsert_batches(records: list[dict], batch_size: int) -> None:
    """Upsert book metadata records in batches via execute_values."""
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            for start in tqdm(range(0, len(records), batch_size), desc="upserting books"):
                batch = records[start : start + batch_size]
                tuples = [tuple(r[c] for c in _COLS) for r in batch]
                execute_values(cur, _UPSERT_SQL, tuples)
        conn.commit()
    finally:
        conn.close()


def load(file_path: Path, batch_size: int = DEFAULT_BATCH_SIZE) -> None:
    """Main entry point.

    Args:
        file_path:  Path to the JSONL books file.
        batch_size: Number of rows per upsert batch.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Books file not found: {file_path}")

    log.info("Fetching known book IDs from database...")
    known_ids = _fetch_known_book_ids()
    log.info("Found %d book stubs in database", len(known_ids))

    records: list[dict] = []
    skipped = 0

    log.info("Streaming %s ...", file_path)
    with open(file_path, "r", encoding="utf-8") as fh:
        for line in tqdm(fh, desc="scanning", unit=" books", mininterval=5.0):
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                continue

            if raw.get("book_id") not in known_ids:
                skipped += 1
                continue

            records.append(_parse_record(raw))

    log.info(
        "Parsed %d matching records (%d skipped — not in interactions set)",
        len(records), skipped,
    )

    if not records:
        log.warning("No matching books found. Is the right file loaded?")
        return

    _upsert_batches(records, batch_size)
    log.info("Book metadata ETL complete — %d books enriched.", len(records))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich book stubs with metadata.")
    parser.add_argument("--file", type=Path, default=DEFAULT_FILE)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    load(file_path=args.file, batch_size=args.batch_size)
