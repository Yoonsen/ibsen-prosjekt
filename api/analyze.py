import os
from contextlib import asynccontextmanager
import sqlite3
from pathlib import Path
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

from api.info_density import InfoDensityConfig, LocalNgramBackend, analyze_text

import shutil

# Absolutt filsti basert på plasseringen til index.py
SOURCE_DB_PATH = Path(__file__).parent / "exports" / "tei_snippets.db"
DB_PATH = Path("/tmp/tei_snippets.db")

class BackendState:
    backend: LocalNgramBackend | None = None

state = BackendState()

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SOURCE_DB_PATH.exists():
        print(f"Advarsel: Database {SOURCE_DB_PATH} ble ikke funnet.")
        yield
        return
        
    if not DB_PATH.exists():
        print(f"Kopierer database til /tmp for skrivetilgang...")
        shutil.copy2(SOURCE_DB_PATH, DB_PATH)
        
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute("SELECT text FROM snippets WHERE text IS NOT NULL AND text != ''").fetchall()
        texts = [str(row[0]) for row in rows]
    finally:
        conn.close()
        
    print(f"Bygger N-gram bakgrunnsmodell med {len(texts)} tekster...")
    state.backend = LocalNgramBackend(texts=texts, n_max=6)
    print("N-gram modell er klar!")
    yield
    state.backend = None

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AnalyzeRequest(BaseModel):
    text: str
    n_min: int = 2
    n_max: int = 6
    threshold: float = 14.0
    top_k: int = 100

@app.post("/api/analyze")
def analyze_endpoint(req: AnalyzeRequest):
    if not state.backend:
        raise HTTPException(status_code=500, detail="Backend er ikke initialisert")
        
    config = InfoDensityConfig(
        n_min=req.n_min,
        n_max=req.n_max,
        threshold=req.threshold,
        top_k=req.top_k
    )
    
    scores, histogram = analyze_text(text=req.text, backend=state.backend, config=config)
    
    candidates = []
    for score in scores:
        candidates.append({
            "phrase": score.phrase,
            "n": score.n,
            "I_score": round(score.info_score, 3),
            "background_count": score.count_background,
            "occurrences_in_selection": score.occurrences_in_selection,
            "sample_sentence": score.sample_sentence,
        })
        
    return {
        "candidates": candidates,
        "histogram": histogram
    }
