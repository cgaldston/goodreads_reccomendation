# Goodreads Recommender System

## Project Status
Early stage — no pipeline built yet, no database connected, no models trained.
Prioritize correctness and clarity over optimization throughout.

## Goal
A web-based book recommender trained on the UCSD Goodreads Book Graph dataset.
New users enter their Goodreads profile URL; their public shelf is scraped to
generate personalized recommendations against the trained model.
This is a portfolio project — architecture and code quality matter.

## Dataset
Source: https://cseweb.ucsd.edu/~jmcauley/datasets/goodreads.html
- 876k users, ~229M user-book interactions, 2.3M books (collected 2017)
- Starting with a single genre subset for development; full dataset for final training
- Key files:
    - goodreads_interactions.csv — user/book interactions (4.1gb)
    - goodreads_books.json.gz — book metadata (2gb)
    - goodreads_book_genres_initial.json.gz — fuzzy genre tags
    - goodreads_reviews_spoiler_raw.json.gz — English reviews (1.3M) for embeddings
- Data is NOT committed to the repo; download steps documented in README

## Planned Stack
- Storage: Supabase (Postgres) + pgvector for embeddings — not yet connected
- Collaborative filtering: `implicit` library (ALS/BPR on sparse matrices)
    - Chosen over `surprise` because of scale: 100M+ interactions require
      a library built for sparse matrix factorization, not surprise's dense approach
- Embeddings: sentence-transformers on review text — not yet implemented
- Scraping: Goodreads public shelf scraper for new user cold-start ingestion
- Backend: FastAPI (tentative)
- Frontend: TBD

## Architecture Notes
- SVD/ALS trains on the UCSD interaction data (offline)
- New users are ingested by scraping their public Goodreads shelf at query time
- Their scraped ratings are projected into the trained latent space for inference
- Embedding store (pgvector) handles content-based fallback for cold-start

## Open Questions / Blockers
- Cloud training setup: ALS on 100M+ interactions needs more than local compute;
  training environment TBD (Colab, Modal, cloud VM, etc.)
- Whether to filter interactions to rated-only (104M) vs. all shelf interactions
- Genre subset to use for initial development (fantasy, history, comics available)
- pgvector in Supabase vs. dedicated vector store (Pinecone, Weaviate, etc.)

## What Needs to Be Built (roughly in order)
1. Data download and EDA (notebooks)
2. SQL schema design + Supabase connection
3. ETL pipeline for books and interactions into Postgres
4. ALS model training via `implicit` (blocked on cloud setup)
5. BERT embeddings for review text + pgvector storage
6. New user ingestion via Goodreads scraper
7. Recommender inference logic (ALS + content fallback)
8. FastAPI backend
9. Frontend

## Project Structure
notebooks/   — EDA and prototyping only; no production logic here
src/
  etl/       — ingestion and transformation scripts
  models/    — ALS training, embedding generation
  api/       — FastAPI app
  scraper/   — Goodreads shelf scraper (new user ingestion)
data/        — local data files; gitignored

## Conventions
- Follow global CLAUDE.md for all style and behavior defaults
- No SELECT * in any query code
- Data files are never committed; document all download/setup steps in README
