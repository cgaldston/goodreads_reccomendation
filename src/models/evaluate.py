"""Comprehensive offline evaluation of the trained ALS model.

Evaluates the trained model against a popularity baseline using four
ranking metrics at cut-off K. Uses the same interaction-level train/test
split as train_als.py (same random seed required for reproducibility).

Metrics reported:
    precision@K  — fraction of top-K recommendations that are relevant
    map@K        — mean average precision (rewards ranking hits higher)
    ndcg@K       — normalized discounted cumulative gain (standard RecSys metric)
    auc@K        — P(random relevant item ranked above random irrelevant item
                    within top-K list)

Usage:
    python -m src.models.evaluate
    python -m src.models.evaluate --k 20 --sample-n 2000 --model-dir models/
"""

import argparse
import logging
import pickle
import sys
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
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

ALPHA = 40.0
DEFAULT_MODEL_DIR = Path("models/")
DEFAULT_K = 10
DEFAULT_SAMPLE_N = 2000
DEFAULT_TRAIN_FRACTION = 0.8
DEFAULT_RANDOM_STATE = 42


# --------------------------------------------------------------------------- #
# Artifact loading
# --------------------------------------------------------------------------- #

def load_artifacts(
    model_dir: Path,
) -> tuple:
    """Load trained model and index mappings from disk.

    Args:
        model_dir: Directory containing als_model.pkl, item_index.pkl,
                   user_index.pkl.

    Returns:
        (model, user_index, item_index)
    """
    model_dir = Path(model_dir)
    missing = [
        f for f in ("als_model.pkl", "item_index.pkl", "user_index.pkl")
        if not (model_dir / f).exists()
    ]
    if missing:
        raise FileNotFoundError(
            f"Missing artifacts in {model_dir}: {missing}. "
            "Run train_als.py first."
        )

    with open(model_dir / "als_model.pkl", "rb") as f:
        model = pickle.load(f)
    with open(model_dir / "user_index.pkl", "rb") as f:
        user_index: dict[str, int] = pickle.load(f)
    with open(model_dir / "item_index.pkl", "rb") as f:
        item_index: dict[str, int] = pickle.load(f)

    log.info(
        "Loaded model: %d users, %d items in index",
        len(user_index), len(item_index),
    )
    return model, user_index, item_index


# --------------------------------------------------------------------------- #
# Matrix construction (using saved indices)
# --------------------------------------------------------------------------- #

def build_matrix(
    user_index: dict[str, int],
    item_index: dict[str, int],
) -> csr_matrix:
    """Build the user-item matrix using saved index mappings from training.

    Interactions not present in the saved indices are silently dropped —
    this should only happen if the DB was modified after training.

    Args:
        user_index: user_id → row index (from training artifacts).
        item_index: book_id → column index (from training artifacts).

    Returns:
        CSR matrix of shape (n_users, n_items).
    """
    log.info("Loading interactions from local Postgres...")
    df = pd.read_sql(
        "SELECT user_id, book_id, user_rating FROM interactions",
        engine,
    )

    # Filter to only users/items present in the saved index
    df = df[df["user_id"].isin(user_index) & df["book_id"].isin(item_index)]

    rows = df["user_id"].map(user_index).values
    cols = df["book_id"].map(item_index).values
    data = (df["user_rating"].astype(float) * ALPHA).values

    user_item = csr_matrix(
        (data, (rows, cols)),
        shape=(len(user_index), len(item_index)),
        dtype=np.float32,
    )
    log.info(
        "Matrix: %d users × %d items, %d non-zeros",
        user_item.shape[0], user_item.shape[1], user_item.nnz,
    )
    return user_item


# --------------------------------------------------------------------------- #
# Per-user ranking metrics
# --------------------------------------------------------------------------- #

def _precision_at_k(recs: list[int], relevant: set[int], k: int) -> float:
    """Fraction of top-K recommendations that are relevant."""
    hits = sum(1 for r in recs[:k] if r in relevant)
    return hits / k


def _ap_at_k(recs: list[int], relevant: set[int], k: int) -> float:
    """Average precision at K.

    Computes the area under the precision-recall curve for the top-K list.
    Rewards models that rank relevant items higher.
    """
    if not relevant:
        return 0.0
    hits = 0
    score = 0.0
    for i, item in enumerate(recs[:k], start=1):
        if item in relevant:
            hits += 1
            score += hits / i
    return score / min(k, len(relevant))


def _ndcg_at_k(recs: list[int], relevant: set[int], k: int) -> float:
    """Normalized discounted cumulative gain at K.

    DCG@K = sum_{i=1}^{K} rel(i) / log2(i + 1)
    NDCG@K = DCG@K / IDCG@K  where IDCG is the ideal (perfect ranking) DCG.
    """
    dcg = sum(
        1.0 / np.log2(i + 2)
        for i, item in enumerate(recs[:k])
        if item in relevant
    )
    idcg = sum(
        1.0 / np.log2(i + 2)
        for i in range(min(k, len(relevant)))
    )
    return dcg / idcg if idcg > 0 else 0.0


def _auc_at_k(recs: list[int], relevant: set[int], k: int) -> float:
    """AUC within the top-K list.

    P(random relevant item in top-K is ranked above a random irrelevant item
    in top-K). Returns 0.0 if there are no hits or all top-K items are hits
    (the statistic is undefined in those cases).
    """
    top_k = recs[:k]
    hit_pos = [i for i, item in enumerate(top_k) if item in relevant]
    miss_pos = [i for i, item in enumerate(top_k) if item not in relevant]
    if not hit_pos or not miss_pos:
        return 0.0
    concordant = sum(1 for h in hit_pos for m in miss_pos if h < m)
    return concordant / (len(hit_pos) * len(miss_pos))


# --------------------------------------------------------------------------- #
# Batch evaluation
# --------------------------------------------------------------------------- #

def _evaluate_recommender(
    recommend_fn: Callable[[int, csr_matrix], list[int]],
    user_item_train: csr_matrix,
    user_item_test: csr_matrix,
    k: int,
    sample_n: int,
    rng: np.random.Generator,
) -> dict[str, float]:
    """Evaluate a recommender function over a sample of test users.

    Args:
        recommend_fn:     Callable(user_id, train_row) → list of item indices.
        user_item_train:  Training matrix (n_users × n_items).
        user_item_test:   Held-out test matrix (same shape).
        k:                Recommendation cut-off.
        sample_n:         Number of users to sample for evaluation.
        rng:              NumPy random generator for reproducible sampling.

    Returns:
        Dict with keys: precision, map, ndcg, auc (mean values across users).
    """
    # Only evaluate users that have at least one held-out interaction
    test_users = np.where(np.diff(user_item_test.indptr) > 0)[0]
    if len(test_users) == 0:
        raise ValueError("No users with test interactions found.")

    sampled = rng.choice(test_users, size=min(sample_n, len(test_users)), replace=False)

    precisions, maps, ndcgs, aucs = [], [], [], []

    for uid in sampled:
        recs = recommend_fn(uid, user_item_train[uid])
        relevant = set(user_item_test[uid].indices)

        precisions.append(_precision_at_k(recs, relevant, k))
        maps.append(_ap_at_k(recs, relevant, k))
        ndcgs.append(_ndcg_at_k(recs, relevant, k))
        aucs.append(_auc_at_k(recs, relevant, k))

    return {
        "precision": float(np.mean(precisions)),
        "map": float(np.mean(maps)),
        "ndcg": float(np.mean(ndcgs)),
        "auc": float(np.mean(aucs)),
    }


# --------------------------------------------------------------------------- #
# Popularity baseline
# --------------------------------------------------------------------------- #

class PopularityRecommender:
    """Recommends globally most popular items, filtered per user.

    Items are ranked by total interaction count in the training set.
    Per-user training interactions are excluded from recommendations.
    """

    def __init__(self, user_item_train: csr_matrix) -> None:
        item_counts = np.asarray(user_item_train.sum(axis=0)).flatten()
        self._popular_items: np.ndarray = np.argsort(item_counts)[::-1]

    def recommend(self, userid: int, train_row: csr_matrix, n: int = 10) -> list[int]:
        """Return top-n most popular items not already in user's training set."""
        already_liked = set(train_row.indices)
        recs = []
        for item in self._popular_items:
            if item not in already_liked:
                recs.append(int(item))
            if len(recs) == n:
                break
        return recs


# --------------------------------------------------------------------------- #
# Main evaluation entry point
# --------------------------------------------------------------------------- #

def evaluate(
    model_dir: Path = DEFAULT_MODEL_DIR,
    k: int = DEFAULT_K,
    sample_n: int = DEFAULT_SAMPLE_N,
    train_fraction: float = DEFAULT_TRAIN_FRACTION,
    random_state: int = DEFAULT_RANDOM_STATE,
) -> dict:
    """Evaluate the trained ALS model against a popularity baseline.

    Reproduces the same interaction-level train/test split used during
    training by seeding numpy with the same random_state.

    Args:
        model_dir:      Directory containing model artifacts.
        k:              Recommendation cut-off for all metrics.
        sample_n:       Number of users to sample for evaluation.
        train_fraction: Must match the value used in train_als.py.
        random_state:   Must match the value used in train_als.py.

    Returns:
        Dict with structure:
            {
                "als":        {"precision": ..., "map": ..., "ndcg": ..., "auc": ...},
                "popularity": {"precision": ..., "map": ..., "ndcg": ..., "auc": ...},
                "k": ...,
                "n_users_evaluated": ...,
            }
    """
    model, user_index, item_index = load_artifacts(Path(model_dir))
    user_item = build_matrix(user_index, item_index)

    # Reproduce the identical split from training
    np.random.seed(random_state)
    user_item_train, user_item_test = implicit_train_test_split(
        user_item, train_percentage=train_fraction
    )
    log.info(
        "Reproduced split: %d train / %d test non-zeros",
        user_item_train.nnz, user_item_test.nnz,
    )

    # Separate RNG for sampling evaluation users (doesn't affect split)
    eval_rng = np.random.default_rng(random_state)

    # --- ALS recommendations ---
    log.info("Evaluating ALS model (K=%d, sample_n=%d)...", k, sample_n)

    def als_recommend(uid: int, train_row: csr_matrix) -> list[int]:
        ids, _ = model.recommend(uid, train_row, N=k, filter_already_liked=True)
        return ids.tolist()

    als_metrics = _evaluate_recommender(
        als_recommend, user_item_train, user_item_test, k, sample_n, eval_rng
    )

    # --- Popularity baseline ---
    log.info("Evaluating popularity baseline (K=%d, sample_n=%d)...", k, sample_n)
    popularity = PopularityRecommender(user_item_train)

    def pop_recommend(uid: int, train_row: csr_matrix) -> list[int]:
        return popularity.recommend(uid, train_row, n=k)

    # Use a fresh RNG with the same seed so both models are evaluated on
    # the same user sample
    eval_rng = np.random.default_rng(random_state)
    pop_metrics = _evaluate_recommender(
        pop_recommend, user_item_train, user_item_test, k, sample_n, eval_rng
    )

    n_eval = min(sample_n, int(np.sum(np.diff(user_item_test.indptr) > 0)))

    return {
        "als": als_metrics,
        "popularity": pop_metrics,
        "k": k,
        "n_users_evaluated": n_eval,
    }


def print_report(results: dict) -> None:
    """Print a formatted side-by-side metrics comparison table."""
    k = results["k"]
    n = results["n_users_evaluated"]
    als = results["als"]
    pop = results["popularity"]

    header = f"{'Metric':<14}{'ALS':>10}{'Popularity':>12}{'Delta':>10}"
    separator = "-" * len(header)

    print(f"\n{'=' * len(header)}")
    print(f"  Evaluation Results  (K={k}, n_users={n})")
    print(f"{'=' * len(header)}")
    print(header)
    print(separator)

    metrics = [
        (f"precision@{k}", "precision"),
        (f"map@{k}",       "map"),
        (f"ndcg@{k}",      "ndcg"),
        (f"auc@{k}",       "auc"),
    ]
    for label, key in metrics:
        a, p = als[key], pop[key]
        delta = a - p
        sign = "+" if delta >= 0 else ""
        print(f"{label:<14}{a:>10.4f}{p:>12.4f}{sign+f'{delta:.4f}':>10}")

    print(separator)
    beats = sum(1 for _, key in metrics if als[key] > pop[key])
    print(f"  ALS beats popularity on {beats}/{len(metrics)} metrics")
    print(f"{'=' * len(header)}\n")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate trained ALS model vs. popularity baseline."
    )
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--k", type=int, default=DEFAULT_K)
    parser.add_argument("--sample-n", type=int, default=DEFAULT_SAMPLE_N)
    parser.add_argument("--train-fraction", type=float, default=DEFAULT_TRAIN_FRACTION)
    parser.add_argument("--random-state", type=int, default=DEFAULT_RANDOM_STATE)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    results = evaluate(
        model_dir=args.model_dir,
        k=args.k,
        sample_n=args.sample_n,
        train_fraction=args.train_fraction,
        random_state=args.random_state,
    )
    print_report(results)
