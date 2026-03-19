"""Train an ALS model on the interactions table.

Reads interactions from local Postgres, builds a sparse user-item matrix,
performs an interaction-level train/test split, trains
implicit.als.AlternatingLeastSquares, logs a quick precision@10 sanity
check, and saves model artifacts to disk.

For a full evaluation report (all four metrics + popularity baseline),
run evaluate.py after training.

Saved artifacts (models/ directory):
    als_model.pkl     — trained AlternatingLeastSquares instance
    item_index.pkl    — dict mapping book_id (str) → item column index (int)
    user_index.pkl    — dict mapping user_id (str) → user row index (int)

Usage:
    python -m src.models.train_als
    python -m src.models.train_als \
        --factors 64 \
        --iterations 30 \
        --regularization 0.01 \
        --train-fraction 0.8 \
        --output-dir models/
"""

import argparse
import logging
import pickle
import sys
from pathlib import Path

import implicit
import numpy as np
import pandas as pd
from implicit.evaluation import precision_at_k as implicit_precision_at_k
from implicit.evaluation import train_test_split as implicit_train_test_split
from scipy.sparse import csr_matrix

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from database.db import engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

ALPHA = 40.0  # confidence scaling: c_ui = 1 + alpha * r_ui  (Hu et al. 2008)


# --------------------------------------------------------------------------- #
# Data loading
# --------------------------------------------------------------------------- #

def load_interactions() -> pd.DataFrame:
    """Load all interactions from local Postgres.

    Returns:
        DataFrame with columns: user_id, book_id, user_rating
    """
    log.info("Loading interactions from local Postgres...")
    df = pd.read_sql(
        "SELECT user_id, book_id, user_rating FROM interactions",
        engine,
    )
    log.info(
        "Loaded %d interactions (%d users, %d books)",
        len(df), df["user_id"].nunique(), df["book_id"].nunique(),
    )
    return df


# --------------------------------------------------------------------------- #
# Filtering
# --------------------------------------------------------------------------- #

def filter_interactions(
    df: pd.DataFrame,
    min_book_ratings: int = 50,
    min_user_ratings: int = 3,
) -> pd.DataFrame:
    """Drop low-signal books and users from the interaction DataFrame.

    Applied after loading so the DB doesn't need to be re-ETL'd when
    thresholds change. Filters are applied iteratively until stable:
    removing books can drop users below min_user_ratings and vice versa.

    Args:
        df:                Raw interactions DataFrame.
        min_book_ratings:  Minimum interactions a book must have to be kept.
        min_user_ratings:  Minimum interactions a user must have to be kept.

    Returns:
        Filtered DataFrame.
    """
    before = len(df)
    for _ in range(10):  # iterate until stable (usually converges in 2-3 passes)
        book_counts = df["book_id"].value_counts()
        df = df[df["book_id"].isin(book_counts[book_counts >= min_book_ratings].index)]
        user_counts = df["user_id"].value_counts()
        df = df[df["user_id"].isin(user_counts[user_counts >= min_user_ratings].index)]
        if len(df) == before:
            break
        before = len(df)

    log.info(
        "After filtering (min_book=%d, min_user=%d): %d interactions / %d users / %d books",
        min_book_ratings, min_user_ratings,
        len(df), df["user_id"].nunique(), df["book_id"].nunique(),
    )
    return df.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Matrix construction
# --------------------------------------------------------------------------- #

def build_sparse_matrix(
    df: pd.DataFrame,
) -> tuple[csr_matrix, dict[str, int], dict[str, int]]:
    """Build a user-item CSR matrix from an interactions DataFrame.

    Confidence values are computed as alpha * rating following the
    linear scaling from Hu et al. 2008.

    Args:
        df: DataFrame with columns user_id, book_id, user_rating.

    Returns:
        user_item:   CSR matrix of shape (n_users, n_items)
        user_index:  user_id → row index
        item_index:  book_id → column index
    """
    user_ids = df["user_id"].unique()
    book_ids = df["book_id"].unique()
    user_index = {u: i for i, u in enumerate(user_ids)}
    item_index = {b: i for i, b in enumerate(book_ids)}

    rows = df["user_id"].map(user_index).values
    cols = df["book_id"].map(item_index).values
    data = (df["user_rating"].astype(float) * ALPHA).values

    user_item = csr_matrix(
        (data, (rows, cols)),
        shape=(len(user_ids), len(book_ids)),
        dtype=np.float32,
    )
    log.info(
        "Sparse matrix: %d users × %d items, density=%.4f%%",
        user_item.shape[0], user_item.shape[1],
        100.0 * user_item.nnz / (user_item.shape[0] * user_item.shape[1]),
    )
    return user_item, user_index, item_index


# --------------------------------------------------------------------------- #
# Training
# --------------------------------------------------------------------------- #

def train(
    factors: int = 64,
    iterations: int = 30,
    regularization: float = 0.01,
    train_fraction: float = 0.8,
    output_dir: Path = Path("models/"),
    random_state: int = 42,
    min_book_ratings: int = 50,
    min_user_ratings: int = 3,
) -> None:
    """Full training pipeline.

    Performs an interaction-level split: for each user, train_fraction of
    their interactions are used for training and the rest are held out for
    evaluation. This ensures every user has latent factors in the trained
    model, unlike a user-level split.

    Args:
        factors:           Number of latent factors.
        iterations:        ALS training iterations.
        regularization:    L2 regularization weight.
        train_fraction:    Fraction of each user's interactions kept for training.
        output_dir:        Directory to save model artifacts.
        random_state:      Seed for reproducibility (must match evaluate.py).
        min_book_ratings:  Minimum interactions a book must have to be included.
        min_user_ratings:  Minimum interactions a user must have to be included.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = load_interactions()
    df = filter_interactions(df, min_book_ratings=min_book_ratings, min_user_ratings=min_user_ratings)
    user_item, user_index, item_index = build_sparse_matrix(df)

    # Interaction-level split: hold out (1 - train_fraction) of each user's
    # interactions. np.random.seed ensures the split is reproducible and
    # matches the split used in evaluate.py.
    np.random.seed(random_state)
    user_item_train, user_item_test = implicit_train_test_split(
        user_item, train_percentage=train_fraction
    )
    log.info(
        "Interaction-level split: %d train / %d test non-zeros",
        user_item_train.nnz, user_item_test.nnz,
    )

    # --- Train ---
    log.info(
        "Training ALS: factors=%d, iterations=%d, regularization=%.4f",
        factors, iterations, regularization,
    )
    model = implicit.als.AlternatingLeastSquares(
        factors=factors,
        iterations=iterations,
        regularization=regularization,
        random_state=random_state,
    )
    model.fit(user_item_train)

    # Quick sanity check — for full metrics run evaluate.py
    p_at_10 = implicit_precision_at_k(
        model, user_item_train, user_item_test, K=10, num_threads=0
    )
    log.info("Precision@10 = %.4f  (run evaluate.py for full metrics report)", p_at_10)
    if p_at_10 < 0.01:
        log.warning(
            "Precision@10 is very low — consider tightening min_book_ratings "
            "or increasing factors/iterations."
        )

    # --- Save artifacts ---
    artifacts = {
        "als_model.pkl": model,
        "item_index.pkl": item_index,
        "user_index.pkl": user_index,
    }
    for filename, obj in artifacts.items():
        path = output_dir / filename
        with open(path, "wb") as f:
            pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)

    log.info("Saved artifacts to %s", output_dir)
    log.info("  als_model.pkl")
    log.info("  item_index.pkl  (%d items)", len(item_index))
    log.info("  user_index.pkl  (%d users)", len(user_index))


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train ALS model on interactions table.")
    parser.add_argument("--factors", type=int, default=64)
    parser.add_argument("--iterations", type=int, default=30)
    parser.add_argument("--regularization", type=float, default=0.01)
    parser.add_argument("--train-fraction", type=float, default=0.8)
    parser.add_argument("--output-dir", type=Path, default=Path("models/"))
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--min-book-ratings", type=int, default=50)
    parser.add_argument("--min-user-ratings", type=int, default=3)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    train(
        factors=args.factors,
        iterations=args.iterations,
        regularization=args.regularization,
        train_fraction=args.train_fraction,
        output_dir=args.output_dir,
        random_state=args.random_state,
        min_book_ratings=args.min_book_ratings,
        min_user_ratings=args.min_user_ratings,
    )
