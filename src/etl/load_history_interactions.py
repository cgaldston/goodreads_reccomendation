"""ETL script: load history/biography genre subset into Supabase.

Input:  data/goodreads_interactions_history_biography.json  (JSONL, ~9.9GB)
Output: interactions, books (stub), and users (stub) tables in Supabase

Usage:
    python -m src.etl.load_history_interactions \
        --file data/goodreads_interactions_history_biography.json \
        --min-user-ratings 5 \
        --min-book-ratings 10 \
        --batch-size 500

Filter logic (from EDA — see notebooks/01_eda_history_subset.ipynb):
    - Keep only rows where is_read=True and rating > 0 (explicit ratings only)
    - Drop users with fewer than MIN_USER_RATINGS explicit ratings
    - Drop books with fewer than MIN_BOOK_RATINGS explicit ratings
    These thresholds cut ~60-70% of the long tail while retaining the dense core
    needed for ALS to produce meaningful factors.
"""

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from database.supabase_client import supabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Defaults (override via CLI flags or EDA findings)
# --------------------------------------------------------------------------- #
DEFAULT_MIN_USER_RATINGS: int = 3
DEFAULT_MIN_BOOK_RATINGS: int = 10
DEFAULT_BATCH_SIZE: int = 2000

# Conflict columns per table — must match PRIMARY KEY constraints in schema.sql
_ON_CONFLICT: dict[str, str] = {
    "users": "user_id",
    "books": "book_id",
    "interactions": "user_id,book_id",
}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _parse_date(raw: Optional[str]) -> Optional[str]:
    """Return ISO date string from a raw Goodreads date field, or None."""
    if not raw:
        return None
    # UCSD format: "Wed Mar 13 10:00:00 -0700 2019"
    try:
        return pd.to_datetime(raw, format="%a %b %d %H:%M:%S %z %Y").date().isoformat()
    except (ValueError, TypeError):
        pass
    # Fallback: let pandas infer
    try:
        return pd.to_datetime(raw).date().isoformat()
    except (ValueError, TypeError):
        return None


def _count_frequencies(
    file_path: Path,
) -> tuple[Counter, Counter, int]:
    """Single-pass over the file to count per-user and per-book explicit ratings.

    Returns:
        user_counts:  Counter mapping user_id → n explicit ratings
        book_counts:  Counter mapping book_id → n explicit ratings
        total_rows:   total rows scanned
    """
    user_counts: Counter = Counter()
    book_counts: Counter = Counter()
    total = 0

    log.info("Pass 1 — counting frequencies (this may take a few minutes for 9.9 GB)...")
    with open(file_path, "r", encoding="utf-8") as fh:
        for line in tqdm(fh, desc="counting", unit=" rows", mininterval=5.0):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not row.get("is_read"):
                continue
            if not row.get("rating", 0):
                continue
            user_counts[row["user_id"]] += 1
            book_counts[row["book_id"]] += 1
            total += 1

    log.info(
        "Pass 1 complete — %d explicit rated interactions, %d unique users, %d unique books",
        total, len(user_counts), len(book_counts),
    )
    return user_counts, book_counts, total


def _build_filtered_df(
    file_path: Path,
    user_counts: Counter,
    book_counts: Counter,
    min_user_ratings: int,
    min_book_ratings: int,
) -> pd.DataFrame:
    """Pass 2 — read only rows that pass frequency filters."""
    valid_users = {u for u, c in user_counts.items() if c >= min_user_ratings}
    valid_books = {b for b, c in book_counts.items() if c >= min_book_ratings}
    log.info(
        "Filter thresholds: users>=%d → %d kept, books>=%d → %d kept",
        min_user_ratings, len(valid_users),
        min_book_ratings, len(valid_books),
    )

    rows = []
    log.info("Pass 2 — loading filtered rows...")
    with open(file_path, "r", encoding="utf-8") as fh:
        for line in tqdm(fh, desc="filtering", unit=" rows", mininterval=5.0):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not row.get("is_read"):
                continue
            rating = row.get("rating", 0)
            if not rating:
                continue
            uid = row["user_id"]
            bid = row["book_id"]
            if uid not in valid_users or bid not in valid_books:
                continue
            rows.append(
                {
                    "user_id": uid,
                    "book_id": bid,
                    "user_rating": int(rating),
                    "date_read": _parse_date(row.get("read_at") or row.get("date_updated")),
                    "shelves": ["read"],
                }
            )

    df = pd.DataFrame(rows)
    # Drop any (user_id, book_id) duplicates — keep highest rating
    df = (
        df.sort_values("user_rating", ascending=False)
        .drop_duplicates(subset=["user_id", "book_id"])
        .reset_index(drop=True)
    )
    log.info(
        "Filtered dataset: %d interactions, %d users, %d books",
        len(df), df["user_id"].nunique(), df["book_id"].nunique(),
    )
    return df


def _upsert_batches(table: str, records: list[dict], batch_size: int) -> None:
    """Upsert a list of dicts into Supabase in batches."""
    on_conflict = _ON_CONFLICT[table]
    for start in tqdm(range(0, len(records), batch_size), desc=f"upserting {table}"):
        batch = records[start : start + batch_size]
        supabase.table(table).upsert(batch, on_conflict=on_conflict).execute()


def load(
    file_path: Path,
    min_user_ratings: int = DEFAULT_MIN_USER_RATINGS,
    min_book_ratings: int = DEFAULT_MIN_BOOK_RATINGS,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """Main entry point.

    Args:
        file_path:        Path to the JSONL interactions file.
        min_user_ratings: Drop users with fewer explicit ratings.
        min_book_ratings: Drop books with fewer explicit ratings.
        batch_size:       Supabase upsert batch size.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    user_counts, book_counts, _ = _count_frequencies(file_path)
    df = _build_filtered_df(
        file_path, user_counts, book_counts, min_user_ratings, min_book_ratings
    )

    # --- 1. Stub users (just the user_id; no other metadata available here) ---
    log.info("Upserting %d stub user records...", df["user_id"].nunique())
    user_records = [{"user_id": uid} for uid in df["user_id"].unique()]
    _upsert_batches("users", user_records, batch_size)

    # --- 2. Stub books (just the book_id; full metadata from goodreads_books.json.gz) ---
    log.info("Upserting %d stub book records...", df["book_id"].nunique())
    book_records = [{"book_id": bid} for bid in df["book_id"].unique()]
    _upsert_batches("books", book_records, batch_size)

    # --- 3. Interactions ---
    log.info("Upserting %d interactions...", len(df))
    interaction_records = df.to_dict(orient="records")
    _upsert_batches("interactions", interaction_records, batch_size)

    log.info("ETL complete.")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load history/bio interactions into Supabase.")
    parser.add_argument(
        "--file",
        type=Path,
        default=Path("data/goodreads_interactions_history_biography.json"),
    )
    parser.add_argument("--min-user-ratings", type=int, default=DEFAULT_MIN_USER_RATINGS)
    parser.add_argument("--min-book-ratings", type=int, default=DEFAULT_MIN_BOOK_RATINGS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    load(
        file_path=args.file,
        min_user_ratings=args.min_user_ratings,
        min_book_ratings=args.min_book_ratings,
        batch_size=args.batch_size,
    )
