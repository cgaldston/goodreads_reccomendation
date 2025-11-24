# api/main.py
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from modeling.recommend import topn
from sqlalchemy import create_engine, text
import os, pandas as pd

app = FastAPI(title="Goodreads Recommender")

# allow local frontends
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

@app.get("/health")
def health(): return {"status": "ok"}

@app.get("/recommendations")
def recommendations(
    user_id: str, n: int = 20, candidate_cap: int = 20000, min_pop: int = 50
):
    return {"user_id": user_id, "results": topn(user_id, n=n,
                                                candidate_cap=candidate_cap,
                                                min_pop=min_pop)}

# cold-start fallback
@app.get("/popular")
def popular(n: int = 20, min_pop: int = 200):
    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('PGUSER','postgres')}:"
        f"{os.getenv('PGPASSWORD','ZippyKiko88')}"
        f"@{os.getenv('PGHOST','localhost')}:{os.getenv('PGPORT','5432')}/"
        f"{os.getenv('PGDATABASE','goodreads')}",
        pool_pre_ping=True,
    )
    with engine.begin() as conn:
        ids = pd.read_sql_query(
            text("""
                SELECT book_id
                FROM mv_popular_items
                WHERE n_ratings >= :min_pop
                ORDER BY n_ratings DESC
                LIMIT :n
            """), conn, params={"min_pop": min_pop, "n": n}
        )["book_id"].tolist()
        meta = pd.read_sql_query(
            text("SELECT book_id, title, image_url FROM book_metadata WHERE book_id = ANY(:ids)"),
            conn, params={"ids": ids}
        )
    return {"results": meta.to_dict(orient="records")}
