from __future__ import annotations

import html
import re
import sqlite3
from pathlib import Path
from typing import Any

import streamlit as st
from info_density import InfoDensityConfig, LocalNgramBackend, analyze_text

DB_PATH = Path("exports/tei_snippets.db")
PROFILE_PATH = Path("exports/tei_metadata_profile.json")


@st.cache_resource
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@st.cache_resource
def build_info_backend(db_path: str) -> LocalNgramBackend:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT text FROM snippets WHERE text IS NOT NULL AND text != ''").fetchall()
        texts = [str(row[0]) for row in rows]
    finally:
        conn.close()
    return LocalNgramBackend(texts=texts, n_max=6)


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


def get_selected_document_texts(conn: sqlite3.Connection, selected_files: list[str]) -> dict[str, str]:
    if not selected_files:
        return {}
    placeholders = ",".join("?" for _ in selected_files)
    sql = f"""
        SELECT source_file, snippet_id, text
        FROM snippets
        WHERE source_file IN ({placeholders}) AND text IS NOT NULL AND text != ''
        ORDER BY source_file, snippet_id
    """
    rows = conn.execute(sql, selected_files).fetchall()
    grouped: dict[str, list[str]] = {source_file: [] for source_file in selected_files}
    for row in rows:
        grouped[str(row["source_file"])].append(str(row["text"]))
    return {source: " ".join(parts) for source, parts in grouped.items() if parts}


def get_documents_by_genre(conn: sqlite3.Connection) -> dict[str, list[str]]:
    rows = conn.execute(
        """
        SELECT genre, source_file
        FROM snippets
        WHERE genre IS NOT NULL AND genre != '' AND source_file IS NOT NULL AND source_file != ''
        GROUP BY genre, source_file
        ORDER BY genre, source_file
        """
    ).fetchall()
    grouped: dict[str, list[str]] = {}
    for row in rows:
        genre = str(row["genre"])
        source_file = str(row["source_file"])
        grouped.setdefault(genre, []).append(source_file)
    return grouped


def get_document_titles(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT source_file, MIN(title) AS title
        FROM snippets
        WHERE source_file IS NOT NULL AND source_file != ''
        GROUP BY source_file
        ORDER BY source_file
        """
    ).fetchall()
    mapping: dict[str, str] = {}
    for row in rows:
        source_file = str(row["source_file"])
        title = str(row["title"]) if row["title"] is not None else ""
        mapping[source_file] = title
    return mapping


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
info_backend = build_info_backend(str(DB_PATH))

tab_search, tab_docs = st.tabs(["Søk i materialet", "Velg dokument"])

with tab_search:
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

with tab_docs:
    st.subheader("Informasjonstetthet (beta)")
    st.caption("Velg dokument(er), beregn I(frase), og få kandidater å jobbe videre med.")

    all_source_files = list_options(conn, "source_file")
    docs_by_genre = get_documents_by_genre(conn)
    doc_titles = get_document_titles(conn)
    genre_options = sorted(docs_by_genre.keys())

    def format_doc_option(source_file: str) -> str:
        title = doc_titles.get(source_file, "").strip()
        if title:
            return f"{title}  ({source_file})"
        return source_file

    selection_mode = st.selectbox(
        "Velg utvalgsmodus",
        options=["Overordnet (sjanger)", "Individuelt (dokument)", "Kombinert"],
        index=2,
    )

    selected_docs_set: set[str] = set()
    if selection_mode in {"Overordnet (sjanger)", "Kombinert"}:
        selected_genres = st.multiselect(
            "Sjanger (overordnet)",
            options=genre_options,
            default=[],
        )
        for genre in selected_genres:
            genre_docs = docs_by_genre.get(genre, [])
            key_name = f"genre_docs_{genre.replace(' ', '_').replace('/', '_')}"
            selected_in_genre = st.multiselect(
                f"{genre} - dokumenter",
                options=genre_docs,
                default=genre_docs,
                format_func=format_doc_option,
                key=key_name,
            )
            selected_docs_set.update(selected_in_genre)

    if selection_mode in {"Individuelt (dokument)", "Kombinert"}:
        selected_individual_docs = st.multiselect(
            "Individuelle dokumenter",
            options=all_source_files,
            default=[],
            format_func=format_doc_option,
            key="selected_individual_docs",
        )
        selected_docs_set.update(selected_individual_docs)

    selected_docs = sorted(selected_docs_set)
    st.caption(f"Valgte dokumenter: {len(selected_docs)}")

    cfg_col_1, cfg_col_2, cfg_col_3, cfg_col_4 = st.columns(4)
    with cfg_col_1:
        n_min = int(st.number_input("n_min", min_value=2, max_value=6, value=2, step=1, key="id_n_min"))
    with cfg_col_2:
        n_max = int(st.number_input("n_max", min_value=2, max_value=6, value=6, step=1, key="id_n_max"))
    with cfg_col_3:
        threshold = float(
            st.number_input("Terskel T", min_value=1.0, max_value=40.0, value=14.0, step=0.5, key="id_t")
        )
    with cfg_col_4:
        top_k = int(st.number_input("Topp kandidater", min_value=10, max_value=500, value=100, step=10, key="id_k"))

    run_density = st.button("Beregn informasjonstetthet")

    if run_density:
        if not selected_docs:
            st.warning("Velg minst ett dokument.")
        elif n_min > n_max:
            st.error("n_min må være mindre enn eller lik n_max.")
        else:
            config = InfoDensityConfig(n_min=n_min, n_max=n_max, threshold=threshold, top_k=top_k)
            doc_texts = get_selected_document_texts(conn, selected_docs)
            if not doc_texts:
                st.warning("Fant ingen tekst i valgte dokumenter.")
            else:
                all_candidates: list[dict[str, Any]] = []
                histogram_total: dict[int, int] = {}

                for source_file, text in doc_texts.items():
                    scores, histogram = analyze_text(text=text, backend=info_backend, config=config)
                    for bucket, count in histogram.items():
                        histogram_total[bucket] = histogram_total.get(bucket, 0) + count
                    for score in scores:
                        all_candidates.append(
                            {
                                "source_file": source_file,
                                "phrase": score.phrase,
                                "n": score.n,
                                "I_score": round(score.info_score, 3),
                                "background_count": score.count_background,
                                "occurrences_in_selection": score.occurrences_in_selection,
                                "sample_sentence": score.sample_sentence,
                            }
                        )

                all_candidates.sort(
                    key=lambda row: (row["I_score"], row["occurrences_in_selection"]),
                    reverse=True,
                )
                all_candidates = all_candidates[:top_k]

                st.metric("Kandidater funnet", len(all_candidates))

                placeholders = ",".join("?" for _ in selected_docs)
                genre_rows = conn.execute(
                    f"""
                    SELECT source_file, genre
                    FROM snippets
                    WHERE source_file IN ({placeholders})
                    GROUP BY source_file, genre
                    """,
                    selected_docs,
                ).fetchall()
                file_to_genre = {str(row["source_file"]): str(row["genre"]) for row in genre_rows}
                by_genre: dict[str, int] = {}
                for candidate in all_candidates:
                    genre = file_to_genre.get(candidate["source_file"], "Unknown")
                    by_genre[genre] = by_genre.get(genre, 0) + 1

                st.markdown("**Kandidatfordeling per sjanger**")
                if by_genre:
                    for genre, count in sorted(by_genre.items(), key=lambda x: x[1], reverse=True):
                        st.write(f"- {genre}: {count}")
                else:
                    st.write("Ingen kandidater over terskel.")

                st.markdown("**Histogram over I-score (heltallsintervaller)**")
                if histogram_total:
                    for bucket, count in sorted(histogram_total.items()):
                        st.write(f"- {bucket}-{bucket + 1}: {count}")
                else:
                    st.write("Ingen verdier å vise.")

                st.markdown("**Toppkandidater**")
                for candidate in all_candidates:
                    header = (
                        f"{candidate['phrase']} | I={candidate['I_score']} | "
                        f"n={candidate['n']} | {candidate['source_file']}"
                    )
                    with st.expander(header):
                        st.write(f"Forekomster i valgt tekst: {candidate['occurrences_in_selection']}")
                        st.write(f"Bakgrunnsfrekvens: {candidate['background_count']}")
                        st.write(candidate["sample_sentence"])
