"""Train an ALS model on the interactions table.

Reads interactions directly from Supabase, builds a sparse user-item matrix,
trains implicit.als.AlternatingLeastSquares, evaluates with precision@k on a
held-out validation split, and saves model artifacts to disk.

Saved artifacts (models/ directory):
    als_model.pkl     — trained AlternatingLeastSquares instance
    item_index.pkl    — dict mapping book_id (str) → item column index (int)
    user_index.pkl    — dict mapping user_id (str) → user row index (int)

Usage:
    python -m src.models.train_als \
        --factors 64 \
        --iterations 30 \
        --regularization 0.01 \
        --val-fraction 0.1 \
        --output-dir models/
"""

import argparse
import logging
import os
import pickle
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.model_selection import train_test_split
from tqdm import tqdm

import implicit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from database.supabase_client import supabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Page size for Supabase queries (max allowed is 1000)
_SUPABASE_PAGE_SIZE = 1000


# --------------------------------------------------------------------------- #
# Data loading
# --------------------------------------------------------------------------- #

def load_interactions() -> pd.DataFrame:
    """Pull all interactions from Supabase in paginated chunks.

    Returns:
        DataFrame with columns: user_id, book_id, user_rating
    """
    log.info("Fetching interactions from Supabase...")
    rows: list[dict] = []
    offset = 0
    while True:
        resp = (
            supabase.table("interactions")
            .select("user_id, book_id, user_rating")
            .range(offset, offset + _SUPABASE_PAGE_SIZE - 1)
            .execute()
        )
        batch = resp.data
        if not batch:
            break
        rows.extend(batch)
        offset += len(batch)
        if len(batch) < _SUPABASE_PAGE_SIZE:
            break

    df = pd.DataFrame(rows)
    log.info("Loaded %d interactions (%d users, %d books)", len(df), df["user_id"].nunique(), df["book_id"].nunique())
    return df


# --------------------------------------------------------------------------- #
# Matrix construction
# --------------------------------------------------------------------------- #

def build_sparse_matrix(
    df: pd.DataFrame,
) -> tuple[csr_matrix, dict[str, int], dict[str, int]]:
    """Build a user-item CSR matrix from an interactions DataFrame.

    The implicit library expects confidence values, not raw ratings.
    We use a simple linear scaling: confidence = alpha * rating.
    alpha=40 is a common starting point (Hu et al. 2008).

    Args:
        df: DataFrame with columns user_id, book_id, user_rating.

    Returns:
        user_item:   CSR matrix of shape (n_users, n_items)
        user_index:  user_id → row index
        item_index:  book_id → column index
    """
    ALPHA = 40.0

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
# Evaluation
# --------------------------------------------------------------------------- #

def precision_at_k(
    model: implicit.als.AlternatingLeastSquares,
    user_item_train: csr_matrix,
    user_item_val: csr_matrix,
    k: int = 10,
    n_eval_users: int = 500,
) -> float:
    """Estimate precision@k on a random sample of validation users.

    For each evaluation user, we recommend K items (excluding training
    interactions) and check how many appear in the held-out val set.

    Args:
        model:            Trained ALS model.
        user_item_train:  Training matrix (n_users × n_items).
        user_item_val:    Validation matrix (same shape).
        k:                Cut-off for precision.
        n_eval_users:     Number of users to sample for evaluation.

    Returns:
        Mean precision@k across sampled users.
    """
    n_users = user_item_train.shape[0]
    # Only evaluate users that have at least one val interaction
    val_users = np.where(np.diff(user_item_val.indptr) > 0)[0]
    if len(val_users) == 0:
        log.warning("No validation users found — skipping precision@k")
        return 0.0

    sample = val_users[: min(n_eval_users, len(val_users))]
    precisions = []

    for uid in tqdm(sample, desc=f"precision@{k}", leave=False):
        rec_ids, _ = model.recommend(
            uid, user_item_train[uid], N=k, filter_already_liked=True
        )
        actual = set(user_item_val[uid].indices)
        hits = len(set(rec_ids) & actual)
        precisions.append(hits / k)

    return float(np.mean(precisions))


# --------------------------------------------------------------------------- #
# Training
# --------------------------------------------------------------------------- #

def train(
    factors: int = 64,
    iterations: int = 30,
    regularization: float = 0.01,
    val_fraction: float = 0.1,
    output_dir: Path = Path("models/"),
    random_state: int = 42,
) -> None:
    """Full training pipeline.

    Args:
        factors:        Number of latent factors.
        iterations:     ALS training iterations.
        regularization: L2 regularization weight.
        val_fraction:   Fraction of users to hold out for evaluation.
        output_dir:     Directory to save model artifacts.
        random_state:   Seed for reproducibility.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Load and split ---
    df = load_interactions()

    # Hold out a fraction of users entirely for validation
    all_users = df["user_id"].unique()
    train_users, val_users = train_test_split(
        all_users, test_size=val_fraction, random_state=random_state
    )
    df_train = df[df["user_id"].isin(train_users)]
    df_val = df[df["user_id"].isin(val_users)]
    log.info(
        "Train: %d interactions / %d users  |  Val: %d interactions / %d users",
        len(df_train), df_train["user_id"].nunique(),
        len(df_val), df_val["user_id"].nunique(),
    )

    # --- Build matrices ---
    # Build on the full set so indices are consistent across train/val
    _, user_index, item_index = build_sparse_matrix(df)

    def _df_to_csr(subset: pd.DataFrame) -> csr_matrix:
        ALPHA = 40.0
        rows = subset["user_id"].map(user_index).values
        cols = subset["book_id"].map(item_index).values
        data = (subset["user_rating"].astype(float) * ALPHA).values
        return csr_matrix(
            (data, (rows, cols)),
            shape=(len(user_index), len(item_index)),
            dtype=np.float32,
        )

    user_item_train = _df_to_csr(df_train)
    user_item_val = _df_to_csr(df_val)

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

    # --- Evaluate ---
    p_at_10 = precision_at_k(model, user_item_train, user_item_val, k=10)
    log.info("Precision@10 = %.4f", p_at_10)

    # --- Save artifacts ---
    model_path = output_dir / "als_model.pkl"
    item_index_path = output_dir / "item_index.pkl"
    user_index_path = output_dir / "user_index.pkl"

    with open(model_path, "wb") as f:
        pickle.dump(model, f, protocol=pickle.HIGHEST_PROTOCOL)
    with open(item_index_path, "wb") as f:
        pickle.dump(item_index, f, protocol=pickle.HIGHEST_PROTOCOL)
    with open(user_index_path, "wb") as f:
        pickle.dump(user_index, f, protocol=pickle.HIGHEST_PROTOCOL)

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
    parser.add_argument("--val-fraction", type=float, default=0.1)
    parser.add_argument("--output-dir", type=Path, default=Path("models/"))
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    train(
        factors=args.factors,
        iterations=args.iterations,
        regularization=args.regularization,
        val_fraction=args.val_fraction,
        output_dir=args.output_dir,
        random_state=args.random_state,
    )
