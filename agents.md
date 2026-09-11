# Ibsen Surprisal & Allusjoner - Agent Context

Dette dokumentet gir kontekst til KI-agenter (eller utviklere) som starter en ny sesjon i dette prosjektet.

## Prosjektets Mål
Prosjektet bruker store språkmodeller (LLM) for å beregne **surprisal** (kryss-entropi-tap) for hver eneste replikk og verselinje i Henrik Ibsens samlede verker. Målet er å finne de mest informasjonstette og "overraskende" formuleringene (gullkorn/allusjoner), og deretter søke opp disse i Nasjonalbibliotekets samlinger (nb.no) for å kartlegge hvordan Ibsens språk har formet norsk kulturhistorie.

## Teknologistakk
- **Frontend:** Next.js (React), TailwindCSS.
- **Backend:** Python (FastAPI).
- **Database:** SQLite (`api/exports/tei_snippets_clean.db`).
- **Data Prep:** Python, lxml (parsing av TEI XML fra Ibsensentret).
- **LLM/AI:** Kjøres i Google Colab (A100 GPU) med `google/gemma-2-9b`.

## Viktige Filer og Mapper
- `/Ibsen-xml/`: Originale TEI XML-filer for Dikt og Drama.
- `rebuild_db.py`: Skript som vasker XML-filene (fjerner `<speaker>`, `<his:hisStage>`) og bygger en fersk SQLite-database.
- `api/analyze.py`: FastAPI-serveren som leser fra `api/exports/tei_snippets_clean.db` og serverer gullkornene.
- `src/app/kildeanalyse/page.tsx`: Hovedgrensesnittet i React. Viser et varmekart av teksten og lar brukeren klikke direkte til `nb.no` globale søk for allusjons-jakt.
- `colab_gemma_surprisal.ipynb`: Jupyter-notebooken som kjøres i skyen for selve AI-regnekraften.

## Slik kjører du prosjektet
For å starte grensesnittet må to terminaler kjøres i prosjektets rotmappe:

**Terminal 1 (Backend - FastAPI):**
```bash
uv run uvicorn api.analyze:app --reload --port 8000
```

**Terminal 2 (Frontend - Next.js):**
```bash
npm run dev
```
Nettsiden åpnes deretter på `http://localhost:3000/kildeanalyse`.

## Status og Veien Videre (Høst 2026)
1. **Surprisal-fasen er ferdig:** 144 000 fraser er beregnet av Gemma 2 9B.
2. **Kultur/Filosofi:** Husk at veldig berømte Ibsen-sitater får lav surprisal (pga. datakontaminering i LLM), mens overraskelsestoppene ofte skjer der subword-tokenizere sliter med 1800-talls rettskriving. (Se gjerne artefakten `prosjekt_oppsummering.md` fra 11. sept 2026 for *Songlines*-analogien).
3. **Neste planlagte steg:**
   - Kjøre utregninger på den helnorske modellen `NbAiLab/borealis2-26b` (krever kvantisering).
   - Implementere Geokoding (NER) for å koble referanser til "stuen", "skogen", "åsen" opp mot geografiske steder (f.eks. Skien, Christiania).
