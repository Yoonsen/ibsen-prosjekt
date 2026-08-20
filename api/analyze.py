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
    conn = sqlite3.connect(DB_PATH)
    try:
        candidates = []
        for score in scores:
            ibsen_match = None
            if score.count_background > 0:
                res = None
                try:
                    # Clean up phrase for FTS5 (escape double quotes just in case)
                    safe_phrase = score.phrase.replace('"', '""')
                    query = f'"{safe_phrase}"'
                    res = conn.execute(
                        "SELECT s.source_file, s.text FROM snippets s "
                        "JOIN snippets_fts f ON s.snippet_id = f.snippet_id "
                        "WHERE f.text MATCH ? LIMIT 1",
                        (query,)
                    ).fetchone()
                except Exception as e:
                    print(f"FTS5 feil for phrase '{score.phrase}': {e}")
                    
                # Robust fallback med LIKE hvis FTS5 (avhengig av Linux-miljø) ikke fant noe
                # (spesielt for særnorske tegn som ø/æ/å som noen tokenizere sliter med)
                if not res:
                    try:
                        res = conn.execute(
                            "SELECT source_file, text FROM snippets "
                            "WHERE text LIKE ? LIMIT 1",
                            (f"%{score.phrase}%",)
                        ).fetchone()
                    except Exception as e:
                        print(f"LIKE feil for phrase '{score.phrase}': {e}")
                
                if res:
                    source = res[0].split('/')[-1].replace('.xml', '') if res[0] else "Ukjent kilde"
                    ibsen_match = f"{source}: {res[1]}"

            candidates.append({
                "phrase": score.phrase,
                "n": score.n,
                "I_score": round(score.info_score, 3),
                "background_count": score.count_background,
                "occurrences_in_selection": score.occurrences_in_selection,
                "sample_sentence": score.sample_sentence,
                "ibsen_match": ibsen_match,
            })
    finally:
        conn.close()
        
    return {
        "candidates": candidates,
        "histogram": histogram
    }


class AnalyzeSourceRequest(BaseModel):
    work_id: str = "Terje Vigen"
    n_min: int = 2
    n_max: int = 6
    threshold: float = 14.0
    top_k: int = 100

import re
import html

@app.get("/api/source-works")
def get_source_works():
    try:
        import urllib.request
        url = "https://raw.githubusercontent.com/Yoonsen/ibsen-prosjekt/main/Ibsen-xml/Dikt/Diktht.xml"
        with urllib.request.urlopen(url) as response:
            text = response.read().decode("utf-8")
    except Exception as e:
        return {"works": []}
    poems = re.split(r'<div[^>]*type="poem"[^>]*>', text)
    
    works = []
    for p in poems[1:]:
        m = re.search(r'<head[^>]*>(.*?)</head>', p, re.DOTALL)
        if m:
            head_html = m.group(1)
            title = html.unescape(re.sub(r'<[^>]+>', '', head_html)).strip()
            if title and title not in works:
                works.append(title)
                
    return {"works": works}

@app.post("/api/analyze-source")
def analyze_source_endpoint(req: AnalyzeSourceRequest):
    if not state.backend:
        raise HTTPException(status_code=500, detail="Backend er ikke initialisert")
        
    try:
        import urllib.request
        url = "https://raw.githubusercontent.com/Yoonsen/ibsen-prosjekt/main/Ibsen-xml/Dikt/Diktht.xml"
        with urllib.request.urlopen(url) as response:
            text = response.read().decode("utf-8")
    except Exception as e:
        raise HTTPException(status_code=404, detail="Kunne ikke laste ned XML")
    poems = re.split(r'<div[^>]*type="poem"[^>]*>', text)
    
    terje_poem = None
    for p in poems:
        if req.work_id in p[:1000]:
            terje_poem = p
            break
            
    if not terje_poem:
        raise HTTPException(status_code=404, detail=f"Diktet {req.work_id} ikke funnet i XML")
        
    p = re.sub(r'<pb[^>]*/>', '', terje_poem)
    p = re.sub(r'<anchor[^>]*/>', '', p)
    p = re.sub(r'<ptr[^>]*/>', '', p)
    
    stanzas = re.findall(r'<HIS:hisLg[^>]*>(.*?)</HIS:hisLg>', p, re.DOTALL)
    if not stanzas:
        stanzas = re.findall(r'<lg[^>]*>(.*?)</lg>', p, re.DOTALL)
        
    extracted_text_blocks = []
    for stanza in stanzas:
        lines = re.findall(r'<l[^>]*>(.*?)</l>', stanza, re.DOTALL)
        clean_lines = [html.unescape(re.sub(r'<[^>]+>', '', l).strip()) for l in lines]
        extracted_text_blocks.append("\n".join(clean_lines))
        
    full_extracted_text = "\n\n".join(extracted_text_blocks)
    
    config = InfoDensityConfig(
        n_min=req.n_min,
        n_max=req.n_max,
        threshold=req.threshold,
        top_k=req.top_k
    )
    
    scores, histogram = analyze_text(text=full_extracted_text, backend=state.backend, config=config)
    
    candidates = []
    for score in scores:
        candidates.append({
            "phrase": score.phrase,
            "n": score.n,
            "I_score": round(score.info_score, 3),
            "background_count": score.count_background,
            "occurrences_in_selection": score.occurrences_in_selection,
            "sample_sentence": score.sample_sentence
        })
        
    return {
        "text": full_extracted_text,
        "candidates": candidates,
        "histogram": histogram
    }
