FROM python:3.12-slim

WORKDIR /app

# Installer uv for rask pakkehåndtering
RUN pip install uv

# Kopier prosjektfiler
COPY pyproject.toml .
COPY uv.lock .

# Installer avhengigheter
RUN uv pip install --system -r pyproject.toml

# Kopier kildekoden (api.py og info_density.py)
COPY api.py .
COPY info_density.py .

# Sørg for at exports-mappen eksisterer (den monteres uansett via docker-compose)
RUN mkdir -p exports

# Kjør uvicorn serveren på port 8000
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
