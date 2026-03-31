from __future__ import annotations

import html
import re
import sqlite3
from pathlib import Path
from typing import Any

import streamlit as st

DB_PATH = Path("exports/tei_snippets.db")
PROFILE_PATH = Path("exports/tei_metadata_profile.json")


@st.cache_resource
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def list_options(conn: sqlite3.Connection, column: str) -> list[str]:
    sql = f"SELECT DISTINCT {column} FROM snippets WHERE {column} IS NOT NULL AND {column} != '' ORDER BY {column}"
    rows = conn.execute(sql).fetchall()
    return [str(row[0]) for row in rows]


def build_filter(
    genres: list[str],
    snippet_types: list[str],
    query: str,
) -> tuple[str, str, list[Any]]:
    where = []
    params: list[Any] = []
    from_clause = "snippets s"

    if query.strip():
        from_clause += " JOIN snippets_fts f ON f.rowid = s.id"
        where.append("f.text MATCH ?")
        params.append(query.strip())

    if genres:
        where.append("s.genre IN ({})".format(",".join("?" for _ in genres)))
        params.extend(genres)
    if snippet_types:
        where.append("s.snippet_type IN ({})".format(",".join("?" for _ in snippet_types)))
        params.extend(snippet_types)

    where_sql = " AND ".join(where)
    return from_clause, where_sql, params


def build_search_query(
    from_clause: str,
    where_sql: str,
    params: list[Any],
    limit: int,
    offset: int,
) -> tuple[str, list[Any]]:
    sql = f"SELECT s.* FROM {from_clause}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    sql += " ORDER BY s.source_file, s.snippet_id LIMIT ? OFFSET ?"
    return sql, [*params, limit, offset]


def count_query(
    conn: sqlite3.Connection,
    from_clause: str,
    where_sql: str,
    params: list[Any],
) -> int:
    sql = f"SELECT COUNT(*) AS c FROM {from_clause}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    row = conn.execute(sql, params).fetchone()
    return int(row["c"]) if row else 0


def breakdown_query(
    conn: sqlite3.Connection,
    from_clause: str,
    where_sql: str,
    params: list[Any],
    field: str,
) -> list[sqlite3.Row]:
    sql = f"SELECT s.{field} AS value, COUNT(*) AS n FROM {from_clause}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    sql += f" GROUP BY s.{field} ORDER BY n DESC, value ASC"
    return conn.execute(sql, params).fetchall()


def search_terms_from_query(query: str) -> list[str]:
    raw = query.strip()
    if not raw:
        return []

    quoted = re.findall(r'"([^"]+)"', raw)
    without_quoted = re.sub(r'"[^"]+"', " ", raw)
    tokens = re.findall(r"[^\s]+", without_quoted)

    ignored = {"and", "or", "not", "near"}
    terms = []
    for part in [*quoted, *tokens]:
        cleaned = part.strip().strip("()")
        if not cleaned:
            continue
        if cleaned.lower() in ignored:
            continue
        # Ignore FTS prefix wildcards in display-highlighting terms.
        cleaned = cleaned.rstrip("*")
        if cleaned:
            terms.append(cleaned)

    # Deduplicate while preserving order.
    seen: set[str] = set()
    result: list[str] = []
    for term in terms:
        key = term.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(term)
    return result


def highlight_text(text: str, terms: list[str]) -> str:
    escaped_text = html.escape(text)
    if not terms:
        return escaped_text

    # Longest terms first to avoid partial overlap replacing longer phrases.
    escaped_terms = [re.escape(html.escape(term)) for term in sorted(terms, key=len, reverse=True) if term]
    if not escaped_terms:
        return escaped_text

    pattern = re.compile("(" + "|".join(escaped_terms) + ")", re.IGNORECASE)
    return pattern.sub(r"<mark>\1</mark>", escaped_text)


st.set_page_config(page_title="Ibsen XML Snippets", layout="wide")
st.title("Ibsen XML Snippets")
st.caption("Søk drives av SQLite FTS5 (ikke vanlig Python-substring). Bruk anførselstegn for frase, f.eks. \"det var\".")
st.markdown(
    """
    <style>
    mark {
      background-color: #b7f5c5;
      color: #0b2e13;
      padding: 0.05em 0.2em;
      border-radius: 0.2em;
      font-weight: 700;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if not DB_PATH.exists():
    st.error("Mangler database: `exports/tei_snippets.db`")
    st.info("Kjør først: `uv run python main.py extract && uv run python main.py profile && uv run python main.py index`")
    st.stop()

conn = get_conn()

query = st.text_input("1) Søk etter ord/frase", placeholder='Eksempel: frihed eller "gamle ven"')
highlight_terms = search_terms_from_query(query)

with st.expander("Valgfrie avgrensninger", expanded=False):
    genres = st.multiselect("Sjanger", list_options(conn, "genre"))
    snippet_types = st.multiselect("Snuttype", list_options(conn, "snippet_type"))
    page_size = st.selectbox("Treff per side", [25, 50, 100, 250], index=1)
    page = st.number_input("Side", min_value=1, value=1, step=1)

from_clause, where_sql, params = build_filter(genres, snippet_types, query)
total = count_query(conn, from_clause, where_sql, params)
offset = (int(page) - 1) * int(page_size)
sql, search_params = build_search_query(from_clause, where_sql, params, int(page_size), offset)
rows = conn.execute(sql, search_params).fetchall()

genre_breakdown = breakdown_query(conn, from_clause, where_sql, params, "genre")
type_breakdown = breakdown_query(conn, from_clause, where_sql, params, "snippet_type")

st.subheader("Treff")
st.metric("Totalt antall treff", total)
if PROFILE_PATH.exists():
    st.caption(f"Metadata-profil: `{PROFILE_PATH}`")

col1, col2 = st.columns(2)
with col1:
    st.markdown("**Fordeling per sjanger**")
    if genre_breakdown:
        for r in genre_breakdown:
            st.write(f"- {r['value']}: {r['n']}")
    else:
        st.write("Ingen treff")
with col2:
    st.markdown("**Fordeling per snippet-type**")
    if type_breakdown:
        for r in type_breakdown:
            st.write(f"- {r['value']}: {r['n']}")
    else:
        st.write("Ingen treff")

st.subheader("Resultater")
for row in rows:
    title = f"{row['snippet_id']} | {row['genre']} | {row['snippet_type']}"
    with st.expander(title):
        highlighted = highlight_text(row["text"] or "", highlight_terms)
        st.markdown(highlighted, unsafe_allow_html=True)
        st.json(
            {
                "doc_id": row["doc_id"],
                "title": row["title"],
                "source_file": row["source_file"],
                "xml_id": row["xml_id"],
                "speaker": row["speaker"],
                "who": row["who"],
                "div_type": row["div_type"],
                "div_n": row["div_n"],
                "div_xml_id": row["div_xml_id"],
                "div_head": row["div_head"],
                "act_n": row["act_n"],
                "act_head": row["act_head"],
                "scene_n": row["scene_n"],
                "scene_head": row["scene_head"],
            }
        )

if not rows and total > 0:
    st.warning("Ingen treff på denne siden. Prøv lavere sidetall.")
elif total == 0:
    st.info("Ingen treff med valgt filter.")
