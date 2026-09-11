import os
import sqlite3
from pathlib import Path
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

# Vercel leser fra exports-mappen direkte. Siden vi ikke lenger bygger
# minne-krevende N-gram ordbøker eller skriver til databasen, 
# trenger vi ikke kopiere den til /tmp.
SOURCE_DB_PATH = Path(__file__).parent / "exports" / "tei_snippets_clean.db"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AnalyzeSourceRequest(BaseModel):
    work_id: str = "Terje Vigen"
    top_k: int = 100

@app.get("/api/source-works")
def get_source_works():
    """Henter alle unike verk som har data i SQLite-basen, gruppert på sjanger."""
    if not SOURCE_DB_PATH.exists():
        return {"grouped_works": {"Dikt": ["Terje Vigen"]}}
        
    conn = sqlite3.connect(SOURCE_DB_PATH)
    try:
        # Finn alle unike titler og sjangere som vi har snippets for
        rows = conn.execute("""
            SELECT DISTINCT s.genre, s.title 
            FROM snippets s
            JOIN snippets_surprisal ss ON s.snippet_id = ss.snippet_id
            WHERE s.title IS NOT NULL AND s.title != ''
            ORDER BY s.genre, s.title
        """).fetchall()
        
        grouped = {}
        for genre, title in rows:
            if genre not in grouped:
                grouped[genre] = []
            grouped[genre].append(title)
            
        works = grouped
    except Exception as e:
        print(f"Feil ved henting av verk: {e}")
        works = {"Dikt": ["Terje Vigen"]}
    finally:
        conn.close()
        
    return {"grouped_works": works}


@app.post("/api/analyze-source")
def analyze_source_endpoint(req: AnalyzeSourceRequest):
    if not SOURCE_DB_PATH.exists():
        raise HTTPException(status_code=500, detail="Database ikke funnet. Har du lastet ned tei_snippets_clean.db fra Colab?")
        
    conn = sqlite3.connect(SOURCE_DB_PATH)
    try:
        search_term = f"%{req.work_id}%"
        
        # Hent de mest overraskende frasene for det valgte verket
        # Vi gjør et lynraskt JOIN mellom tekstsnuttene og LLM-logitene
        query = """
            SELECT ss.phrase, ss.surprisal_score, s.text, s.snippet_id
            FROM snippets_surprisal ss
            JOIN snippets s ON ss.snippet_id = s.snippet_id
            WHERE s.title LIKE ? OR s.doc_id LIKE ? OR s.source_file LIKE ?
            ORDER BY ss.surprisal_score DESC
            LIMIT ?
        """
        rows = conn.execute(query, (search_term, search_term, search_term, req.top_k)).fetchall()
        
        candidates = []
        for row in rows:
            candidates.append({
                "phrase": row[0],
                "I_score": row[1], # Beholder feltnavnet 'I_score' så frontend slipper endring
                "sample_sentence": row[2],
                "snippet_id": row[3],
                "background_count": 0, 
                "occurrences_in_selection": 1,
            })
            
        # For å unngå å sende 600+ KB med brevtekst til frontend (som får nettleseren til å krasje 
        # når React prøver å regex-markere alt), bygger vi bare teksten fra de unike snuttene som har gullkorn!
        text_blocks = []
        for row in rows:
            if row[2] and row[2] not in text_blocks:
                text_blocks.append(row[2])
                
        full_extracted_text = "\n\n[...]\n\n".join(text_blocks)
        
    finally:
        conn.close()
        
    if not candidates:
        raise HTTPException(status_code=404, detail=f"Ingen LLM-analyserte fraser funnet for {req.work_id}")
        
    return {
        "text": full_extracted_text,
        "candidates": candidates,
        "histogram": {} # Fases ut
    }

class AnalyzeRequest(BaseModel):
    text: str
    top_k: int = 100

@app.post("/api/analyze")
def analyze_endpoint(req: AnalyzeRequest):
    # Siden vi nå bruker pre-komputerte LLM-logits (Surprisal), 
    # støtter vi ikke lenger å lime inn ukjente tekster live i Vercel
    # (det ville krevd at Vercel kjørte Gemma 2B on-the-fly).
    return {
        "candidates": [{
            "phrase": "Fritekstanalyse er deaktivert, da modellen nå kjører lokalt via Colab.",
            "I_score": 99.9,
            "sample_sentence": req.text,
            "snippet_id": "info"
        }],
        "histogram": {}
    }
