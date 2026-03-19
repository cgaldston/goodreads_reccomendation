"""Minimum-bar performance tests for the trained ALS model.

These are integration tests: they require a trained model (run train_als.py
first) and an active local Postgres connection. They will be skipped
automatically if model artifacts are not present.

Run with:
    pytest tests/test_model_performance.py -v

All tests share a single evaluate() call (module-scoped fixture) so the
DB query and split only happen once per test session.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.models.evaluate import evaluate

MODEL_DIR = Path("models/")
SAMPLE_N = 500   # keep fast; evaluate.py default is 2000
K = 10


@pytest.fixture(scope="module")
def results():
    """Run full evaluation once and share results across all tests."""
    missing = [
        f for f in ("als_model.pkl", "item_index.pkl", "user_index.pkl")
        if not (MODEL_DIR / f).exists()
    ]
    if missing:
        pytest.skip(f"Model artifacts not found in {MODEL_DIR} — run train_als.py first.")

    return evaluate(model_dir=MODEL_DIR, k=K, sample_n=SAMPLE_N, random_state=42)


# --------------------------------------------------------------------------- #
# Sanity checks
# --------------------------------------------------------------------------- #

def test_results_have_expected_keys(results):
    """evaluate() output has the expected structure."""
    assert "als" in results
    assert "popularity" in results
    for key in ("precision", "map", "ndcg", "auc"):
        assert key in results["als"], f"Missing ALS metric: {key}"
        assert key in results["popularity"], f"Missing popularity metric: {key}"


def test_all_metrics_are_non_negative(results):
    """All metric values should be >= 0."""
    for model_key in ("als", "popularity"):
        for metric, value in results[model_key].items():
            assert value >= 0.0, f"{model_key}.{metric} = {value} (expected >= 0)"


def test_all_metrics_are_at_most_one(results):
    """All metric values should be <= 1."""
    for model_key in ("als", "popularity"):
        for metric, value in results[model_key].items():
            assert value <= 1.0, f"{model_key}.{metric} = {value} (expected <= 1)"


# --------------------------------------------------------------------------- #
# Minimum absolute thresholds
# --------------------------------------------------------------------------- #

def test_minimum_precision(results):
    """ALS precision@10 must exceed a minimum acceptable floor.

    A precision@10 below 0.01 means fewer than 1 in 100 recommendations
    are relevant — effectively random. If this fails, consider tightening
    min_book_ratings or increasing factors/iterations.
    """
    p = results["als"]["precision"]
    assert p > 0.01, f"ALS precision@{K} = {p:.4f} — below minimum threshold of 0.01"


def test_minimum_ndcg(results):
    """ALS NDCG@10 must exceed a minimum acceptable floor."""
    ndcg = results["als"]["ndcg"]
    assert ndcg > 0.02, f"ALS ndcg@{K} = {ndcg:.4f} — below minimum threshold of 0.02"


# --------------------------------------------------------------------------- #
# ALS must beat the popularity baseline
# --------------------------------------------------------------------------- #

def test_als_beats_popularity_precision(results):
    """ALS should outperform a naive popularity baseline on precision@K."""
    als_p = results["als"]["precision"]
    pop_p = results["popularity"]["precision"]
    assert als_p > pop_p, (
        f"ALS precision@{K} ({als_p:.4f}) does not beat popularity ({pop_p:.4f}). "
        "The model is not adding value over a trivial baseline."
    )


def test_als_beats_popularity_ndcg(results):
    """ALS should outperform a naive popularity baseline on NDCG@K."""
    als_ndcg = results["als"]["ndcg"]
    pop_ndcg = results["popularity"]["ndcg"]
    assert als_ndcg > pop_ndcg, (
        f"ALS ndcg@{K} ({als_ndcg:.4f}) does not beat popularity ({pop_ndcg:.4f}). "
        "The model is not adding value over a trivial baseline."
    )


def test_als_beats_popularity_map(results):
    """ALS should outperform a naive popularity baseline on MAP@K."""
    als_map = results["als"]["map"]
    pop_map = results["popularity"]["map"]
    assert als_map > pop_map, (
        f"ALS map@{K} ({als_map:.4f}) does not beat popularity ({pop_map:.4f}). "
        "The model is not adding value over a trivial baseline."
    )
