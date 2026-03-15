"""New-user inference: fold a scraped shelf into the trained ALS latent space.

The trained model covers the UCSD history/biography subset (dataset circa 2019).
Any book_id scraped from Goodreads that does not appear in the trained item index
is filtered out before inference. This is intentional — we log the drop rate so
you can understand recommendation quality at runtime.

Typical usage:
    from src.models.recommend import get_recommendations

    scraped = [("123456", 4), ("789012", 5), ("999999", 3)]
    results = get_recommendations(scraped, n=20)
    # results is a list of dicts with book metadata
"""

import logging
import pickle
import sys
from pathlib import Path
from typing import Optional

import numpy as np
from scipy.sparse import csr_matrix

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from database.supabase_client import supabase

log = logging.getLogger(__name__)

# Default artifact paths — override by passing explicit paths to load_artifacts()
_DEFAULT_MODEL_DIR = Path("models/")

# Module-level cache so artifacts are only loaded once per process
_model = None
_item_index: Optional[dict[str, int]] = None  # book_id → col index
_item_ids: Optional[list[str]] = None          # col index → book_id


def load_artifacts(model_dir: Path = _DEFAULT_MODEL_DIR) -> None:
    """Load ALS model and item index into module-level cache.

    Call this once at startup (e.g., from the FastAPI lifespan handler).
    Subsequent calls are no-ops if artifacts are already loaded.

    Args:
        model_dir: Directory containing als_model.pkl and item_index.pkl.
    """
    global _model, _item_index, _item_ids

    if _model is not None:
        return  # already loaded

    model_dir = Path(model_dir)

    model_path = model_dir / "als_model.pkl"
    item_index_path = model_dir / "item_index.pkl"

    if not model_path.exists() or not item_index_path.exists():
        raise FileNotFoundError(
            f"Model artifacts not found in {model_dir}. "
            "Run `python -m src.models.train_als` first."
        )

    with open(model_path, "rb") as f:
        _model = pickle.load(f)
    with open(item_index_path, "rb") as f:
        _item_index = pickle.load(f)

    # Inverse map: column index → book_id  (for decoding recommendations)
    _item_ids = [None] * len(_item_index)
    for book_id, idx in _item_index.items():
        _item_ids[idx] = book_id

    log.info("ALS artifacts loaded: %d items in index", len(_item_index))


def _fetch_book_metadata(book_ids: list[str]) -> list[dict]:
    """Fetch book metadata from Supabase for a list of book_ids.

    Args:
        book_ids: List of Goodreads book_id strings.

    Returns:
        List of dicts with keys: book_id, title, author_name, average_rating,
        ratings_count, cover_image_url.  Books not found in the DB are omitted.
    """
    if not book_ids:
        return []
    resp = (
        supabase.table("books")
        .select("book_id, title, author_name, average_rating, ratings_count, cover_image_url")
        .in_("book_id", book_ids)
        .execute()
    )
    return resp.data or []


def get_recommendations(
    scraped_interactions: list[tuple[str, int]],
    n: int = 20,
    model_dir: Path = _DEFAULT_MODEL_DIR,
) -> list[dict]:
    """Generate top-N recommendations for a new user.

    The user is "folded in" to the trained latent space without retraining.
    Scraped books that don't exist in the item index are dropped (logged).

    Args:
        scraped_interactions: List of (book_id, rating) pairs from the user's
            Goodreads shelf. Ratings should be 1–5 integers.
        n:                    Number of recommendations to return.
        model_dir:            Directory containing model artifacts.

    Returns:
        List of book metadata dicts, ordered by recommendation score.
        Each dict has: book_id, title, author_name, average_rating,
        ratings_count, cover_image_url.

    Raises:
        FileNotFoundError: If model artifacts have not been trained yet.
        ValueError:        If no scraped books overlap with the item index.
    """
    load_artifacts(model_dir)

    # --- Filter to known items ---
    known = [(bid, r) for bid, r in scraped_interactions if bid in _item_index]
    n_scraped = len(scraped_interactions)
    n_known = len(known)
    n_dropped = n_scraped - n_known

    log.info(
        "Shelf coverage: %d/%d books found in item index (%.0f%% coverage, %d dropped)",
        n_known, n_scraped,
        100.0 * n_known / n_scraped if n_scraped else 0,
        n_dropped,
    )

    if not known:
        raise ValueError(
            f"None of the {n_scraped} scraped books appear in the trained item index. "
            "The model was trained on the history/biography subset (UCSD 2019 data). "
            "This user's shelf may not overlap with that genre subset."
        )

    # --- Build new-user sparse vector (1 × n_items) ---
    # implicit expects confidence values; use the same alpha=40 as training
    ALPHA = 40.0
    col_indices = [_item_index[bid] for bid, _ in known]
    confidences = [float(r) * ALPHA for _, r in known]

    user_items = csr_matrix(
        (confidences, ([0] * len(col_indices), col_indices)),
        shape=(1, len(_item_index)),
        dtype=np.float32,
    )

    # --- Fold in: recommend for row 0 of user_items ---
    # implicit.recommend excludes items already in user_items by default
    rec_indices, rec_scores = _model.recommend(
        0,
        user_items,
        N=n,
        filter_already_liked=True,
    )

    # --- Decode to book_ids ---
    recommended_ids = [_item_ids[i] for i in rec_indices]
    log.info("Top recommendation scores: %s", rec_scores[:5].tolist())

    # --- Fetch metadata ---
    metadata = _fetch_book_metadata(recommended_ids)

    # Preserve recommendation order
    meta_by_id = {m["book_id"]: m for m in metadata}
    ordered = [meta_by_id[bid] for bid in recommended_ids if bid in meta_by_id]

    # Attach recommendation rank for the caller's convenience
    for rank, rec in enumerate(ordered, start=1):
        rec["rank"] = rank

    return ordered
