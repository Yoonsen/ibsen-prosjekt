# Ibsen XML - manifest, uttrekk og mini-app

## Kommandoer

- Generer entity-manifest:
  - `uv run python main.py manifest`
- Ekstraher tekstsnutter til JSONL:
  - `uv run python main.py extract`
- Lag metadata-profil per sjanger:
  - `uv run python main.py profile`
- Bygg SQLite-indeks for app/sok:
  - `uv run python main.py index`
- Start mini-app (Streamlit):
  - `uv run streamlit run app.py`

## Informasjonstetthet (beta)

- Appen har en beta-seksjon for informasjonstetthet (`I(frase)`).
- Velg dokument(er), sett `n_min/n_max`, terskel `T` og antall toppkandidater.
- Analysen bruker en lokal n-gram-bakgrunn bygget fra `snippets`-materialet, og returnerer kandidater med hoy `I`-score.

## Filer som lages

- `manifest/tei_manifest.json`
- `exports/tei_snippets.jsonl`
- `exports/tei_snippets_summary.json`
- `exports/tei_metadata_profile.json`
- `exports/tei_snippets.db`

## Merknader

- `Ibsen-xml` holdes ren med bare XML-filer.
- Appen bruker SQLite + FTS (backend-stil) i stedet for a laste alle data i nettleseren.
