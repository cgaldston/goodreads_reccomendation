# api/main.py
import logging
import os
import re
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import pandas as pd

# Allow imports from project root when running with uvicorn from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.models.recommend import get_recommendations, load_artifacts
from scrapers.user_interactions_scraper import GoodreadsUserInteractionsScraper

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Lifespan: load model artifacts once at startup
# --------------------------------------------------------------------------- #

@asynccontextmanager
async def lifespan(app: FastAPI):
    model_dir = Path(os.getenv("MODEL_DIR", "models/"))
    try:
        load_artifacts(model_dir)
        log.info("ALS artifacts loaded from %s", model_dir)
    except FileNotFoundError as exc:
        log.warning("Model artifacts not found — /recommendations will be unavailable: %s", exc)
    yield


# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #

app = FastAPI(title="Goodreads Recommender", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_PROFILE_URL_RE = re.compile(r"goodreads\.com/user/show/(\d+)")


def _engine():
    """Build a SQLAlchemy engine from environment variables.

    Required env vars: PGUSER, PGPASSWORD, PGHOST, PGPORT, PGDATABASE.
    Never hardcode credentials — set these in your shell or a .env file.
    """
    for var in ("PGUSER", "PGPASSWORD", "PGHOST", "PGPORT", "PGDATABASE"):
        if not os.getenv(var):
            raise EnvironmentError(
                f"Missing required env var: {var}. "
                "Set all PG* variables before starting the server."
            )
    url = (
        f"postgresql+psycopg2://{os.environ['PGUSER']}:{os.environ['PGPASSWORD']}"
        f"@{os.environ['PGHOST']}:{os.environ['PGPORT']}/{os.environ['PGDATABASE']}"
    )
    return create_engine(url, pool_pre_ping=True)


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/recommendations")
def recommendations(
    profile_url: str = Query(..., description="Public Goodreads profile URL"),
    n: int = Query(20, ge=1, le=100),
):
    """Scrape a user's Goodreads shelf and return personalised recommendations.

    Steps:
        1. Extract user_id from the profile URL.
        2. Scrape their public 'read' shelf via GoodreadsUserInteractionsScraper.
        3. Filter scraped books to those in the trained item index.
        4. Fold the user into the ALS latent space and retrieve top-N recommendations.
        5. Return book metadata ordered by recommendation score.
    """
    match = _PROFILE_URL_RE.search(profile_url)
    if not match:
        raise HTTPException(
            status_code=400,
            detail="Could not parse a Goodreads user_id from the provided URL. "
                   "Expected format: https://www.goodreads.com/user/show/<id>",
        )
    user_id = match.group(1)

    # Scrape shelf — returns List[Interaction] dataclass objects
    scraper = GoodreadsUserInteractionsScraper()
    try:
        interactions_raw = scraper.scrape_user_interactions(user_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Shelf scrape failed: {exc}") from exc

    if not interactions_raw:
        raise HTTPException(
            status_code=404,
            detail="No interactions found on this user's public shelf. "
                   "The shelf may be private or the user has no 'read' books.",
        )

    # Convert Interaction dataclass objects to (book_id, rating) pairs; skip unrated
    scraped = [
        (str(interaction.book_id), int(interaction.user_rating))
        for interaction in interactions_raw
        if interaction.book_id and interaction.user_rating
    ]

    if not scraped:
        raise HTTPException(
            status_code=422,
            detail="Shelf found but no explicit ratings detected. "
                   "The model requires at least one rated book to generate recommendations.",
        )

    try:
        results = get_recommendations(scraped, n=n)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=503,
            detail="Model not loaded. Run `python -m src.models.train_als` first.",
        ) from exc

    return {
        "user_id": user_id,
        "n_shelf_books": len(interactions_raw),
        "n_rated": len(scraped),
        "results": results,
    }


@app.get("/popular")
def popular(
    n: int = Query(20, ge=1, le=100),
    min_pop: int = Query(200, ge=1),
):
    """Cold-start fallback: return the most-rated books in the dataset."""
    try:
        engine = _engine()
    except EnvironmentError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    with engine.begin() as conn:
        ids = pd.read_sql_query(
            text("""
                SELECT book_id
                FROM mv_popular_items
                WHERE n_ratings >= :min_pop
                ORDER BY n_ratings DESC
                LIMIT :n
            """),
            conn,
            params={"min_pop": min_pop, "n": n},
        )["book_id"].tolist()

        meta = pd.read_sql_query(
            text("""
                SELECT book_id, title, author_name, cover_image_url
                FROM books
                WHERE book_id = ANY(:ids)
            """),
            conn,
            params={"ids": ids},
        )

    return {"results": meta.to_dict(orient="records")}
