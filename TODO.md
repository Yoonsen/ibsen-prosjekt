# TODO

## Videreutvikling av Allusjonsdeteksjon
- [ ] Utvikle et lokalt script (kjøres på Mac/GPU-maskin) som tar tekstene, mater dem gjennom en liten LLM (f.eks. via Hugging Face/PyTorch), og henter ut logits/surprisal per token/frase.
- [ ] Lagre disse forhåndsberegnede resultatene (tekst + logits) i en SQLite-database.
- [ ] Utvikle pipeline som identifiserer sekvenser med uventet høy surprisal/informasjonstetthet som potensielle allusjoner basert på SQLite-dataene.
- [ ] Sende disse kandidat-ankrene videre til ElasticSearch (api.nb.no / Nettbiblioteket) for verifisering og kontekstsøk.

## Infrastruktur & Vercel
- [ ] Utvikle en Google Colab notebook for LLM-prosessering. Kjører Gemma-2-2b på gratis GPU, leser SQLite-basen, beregner surprisal og oppdaterer databasen.
- [ ] Tilpasse Vercel/FastAPI-appen til å lese de forhåndsberegnede logitene. Databasen kan hostes lokalt hos NB (Nasjonalbiblioteket) for å omgå Vercels lagringsbegrensninger (og den 5GB store ord-databasen kan også ligge der).
- [ ] Gå gjennom, parse og trekke ut tekst fra `Skolen og Ibsen.pdf` for bruk i analysen.
