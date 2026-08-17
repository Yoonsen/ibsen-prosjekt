from __future__ import annotations

import argparse
import json
import re
import sqlite3
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lxml import etree

ROOT_DIR = Path("Ibsen-xml")
MANIFEST_OUTPUT_DIR = Path("manifest")
MANIFEST_OUTPUT_FILE = MANIFEST_OUTPUT_DIR / "tei_manifest.json"
EXPORT_OUTPUT_DIR = Path("exports")
SNIPPETS_OUTPUT_FILE = EXPORT_OUTPUT_DIR / "tei_snippets.jsonl"
SNIPPETS_SUMMARY_FILE = EXPORT_OUTPUT_DIR / "tei_snippets_summary.json"
METADATA_PROFILE_FILE = EXPORT_OUTPUT_DIR / "tei_metadata_profile.json"
INDEX_DB_FILE = EXPORT_OUTPUT_DIR / "tei_snippets.db"
XML_GLOB = "*.xml"

PREDEFINED_XML_ENTITIES = {"amp", "lt", "gt", "quot", "apos"}

ENTITY_DECL_RE = re.compile(
    r"<!ENTITY\s+"
    r"(?:(?P<parameter>%\s*)?)"
    r"(?P<name>[A-Za-z_:][A-Za-z0-9._:-]*)\s+"
    r"(?:(?P<external_type>SYSTEM|PUBLIC)\s+(?P<external_id>(?:\"[^\"]*\"|'[^']*')(?:\s+(?:\"[^\"]*\"|'[^']*'))?)|(?P<value>\"[^\"]*\"|'[^']*'))\s*>",
    re.IGNORECASE,
)
GENERAL_ENTITY_REF_RE = re.compile(r"&([A-Za-z_:][A-Za-z0-9._:-]*);")
PARAM_ENTITY_REF_RE = re.compile(r"%\s*([A-Za-z_:][A-Za-z0-9._:-]*)\s*;")
QUOTED_STRING_RE = re.compile(r"""['"]([^'"]+)['"]""")
DOCTYPE_BLOCK_RE = re.compile(r"<!DOCTYPE[\s\S]*?\]>", re.IGNORECASE)
DOCTYPE_SIMPLE_RE = re.compile(r"<!DOCTYPE[^>]*>", re.IGNORECASE)
ORPHANED_DTD_LINE_RE = re.compile(r"^\s*(%\s*[A-Za-z_:][A-Za-z0-9._:-]*\s*;|\]>)\s*$", re.MULTILINE)

TEI_NS = {"tei": "http://www.tei-c.org/ns/1.0", "xml": "http://www.w3.org/XML/1998/namespace"}


@dataclass(frozen=True)
class EntityDefinition:
    name: str
    kind: str  # "general" or "parameter"
    source: str  # "internal", "external_dtd", "predefined"
    file: str
    external_url: str | None = None
    value_preview: str | None = None


def parse_entity_declarations(text: str, file_path: str, source: str) -> list[EntityDefinition]:
    definitions: list[EntityDefinition] = []
    for match in ENTITY_DECL_RE.finditer(text):
        name = match.group("name")
        is_parameter = bool(match.group("parameter"))
        external_id_raw = match.group("external_id")
        external_url = None
        if external_id_raw:
            quoted = QUOTED_STRING_RE.findall(external_id_raw)
            if quoted:
                # For SYSTEM this is the first quoted string. For PUBLIC there may be two.
                external_url = quoted[-1] if len(quoted) > 1 else quoted[0]
        value = match.group("value")
        value_preview = None
        if value:
            value_preview = value.strip("'\"")
            if len(value_preview) > 120:
                value_preview = f"{value_preview[:117]}..."
        definitions.append(
            EntityDefinition(
                name=name,
                kind="parameter" if is_parameter else "general",
                source=source,
                file=file_path,
                external_url=external_url,
                value_preview=value_preview,
            )
        )
    return definitions


def fetch_text(url: str) -> tuple[str | None, str | None]:
    request = urllib.request.Request(url, headers={"User-Agent": "ibsen-manifest/0.1"})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.read().decode("utf-8", errors="replace"), None
    except urllib.error.URLError as exc:
        return None, str(exc)


def build_manifest() -> dict:
    xml_files = sorted(ROOT_DIR.rglob(XML_GLOB))
    if not xml_files:
        raise FileNotFoundError(f"No XML files found under {ROOT_DIR}")

    files_info: list[dict] = []
    unique_external_dtd_urls: set[str] = set()
    external_dtd_errors: dict[str, str] = {}
    external_dtd_definitions: dict[str, list[EntityDefinition]] = {}

    max_depth = 0
    deeper_than_one_level: list[str] = []

    for xml_file in xml_files:
        rel = xml_file.relative_to(ROOT_DIR)
        depth = len(rel.parts) - 1
        max_depth = max(max_depth, depth)
        if depth > 1:
            deeper_than_one_level.append(rel.as_posix())

        text = xml_file.read_text(encoding="utf-8", errors="replace")
        internal_defs = parse_entity_declarations(text, rel.as_posix(), source="internal")
        local_general_defs = sorted({d.name for d in internal_defs if d.kind == "general"})
        local_parameter_defs = sorted({d.name for d in internal_defs if d.kind == "parameter"})

        param_refs = sorted(set(PARAM_ENTITY_REF_RE.findall(text)))
        all_general_refs = set(GENERAL_ENTITY_REF_RE.findall(text))
        general_refs = sorted(all_general_refs - PREDEFINED_XML_ENTITIES)

        external_urls = sorted({d.external_url for d in internal_defs if d.external_url})
        unique_external_dtd_urls.update(external_urls)

        files_info.append(
            {
                "file": rel.as_posix(),
                "subfolder": rel.parts[0] if rel.parts else "",
                "depth_from_root": depth,
                "has_doctype": "<!DOCTYPE" in text,
                "local_entity_definitions": {
                    "general": local_general_defs,
                    "parameter": local_parameter_defs,
                },
                "entity_references": {
                    "general": general_refs,
                    "parameter": param_refs,
                },
                "external_dtd_urls": external_urls,
            }
        )

    for url in sorted(unique_external_dtd_urls):
        dtd_text, err = fetch_text(url)
        if err:
            external_dtd_errors[url] = err
            external_dtd_definitions[url] = []
            continue
        external_dtd_definitions[url] = parse_entity_declarations(dtd_text or "", url, source="external_dtd")

    total_unresolved = 0
    for file_info in files_info:
        resolved_general = set(file_info["local_entity_definitions"]["general"])
        for url in file_info["external_dtd_urls"]:
            defs = external_dtd_definitions.get(url, [])
            resolved_general.update(d.name for d in defs if d.kind == "general")
        unresolved = sorted(set(file_info["entity_references"]["general"]) - resolved_general)
        file_info["resolution"] = {
            "resolved_general_entities_count": len(file_info["entity_references"]["general"]) - len(unresolved),
            "unresolved_general_entities": unresolved,
        }
        total_unresolved += len(unresolved)

    external_summary = {}
    for url, defs in external_dtd_definitions.items():
        external_summary[url] = {
            "general_entities": sorted({d.name for d in defs if d.kind == "general"}),
            "parameter_entities": sorted({d.name for d in defs if d.kind == "parameter"}),
            "error": external_dtd_errors.get(url),
        }

    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "root": ROOT_DIR.as_posix(),
        "checks": {
            "file_count": len(xml_files),
            "max_subfolder_depth": max_depth,
            "all_files_within_one_subfolder_level": len(deeper_than_one_level) == 0,
            "files_deeper_than_one_level": deeper_than_one_level,
        },
        "entity_resolution_summary": {
            "files_with_unresolved_general_entities": sum(
                1 for f in files_info if f["resolution"]["unresolved_general_entities"]
            ),
            "total_unresolved_general_entities": total_unresolved,
        },
        "external_dtds": external_summary,
        "files": files_info,
    }


def write_manifest() -> None:
    manifest = build_manifest()
    MANIFEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_OUTPUT_FILE.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote manifest: {MANIFEST_OUTPUT_FILE}")
    print(
        "Depth check:",
        "OK" if manifest["checks"]["all_files_within_one_subfolder_level"] else "Needs attention",
        f"(max depth={manifest['checks']['max_subfolder_depth']})",
    )
    print(
        "Unresolved general entities:",
        manifest["entity_resolution_summary"]["total_unresolved_general_entities"],
    )


def clean_text_for_parse(raw_xml: str) -> str:
    without_doctype = DOCTYPE_BLOCK_RE.sub("", raw_xml)
    without_doctype = DOCTYPE_SIMPLE_RE.sub("", without_doctype)
    without_doctype = ORPHANED_DTD_LINE_RE.sub("", without_doctype)

    def replace_entity(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in PREDEFINED_XML_ENTITIES:
            return match.group(0)
        # Keep document parseable even if external/internal entity definitions are missing.
        return " "

    return GENERAL_ENTITY_REF_RE.sub(replace_entity, without_doctype)


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def first_text(root: etree._Element, xpath_expr: str) -> str | None:
    result = root.xpath(xpath_expr, namespaces=TEI_NS)
    if result is None:
        return None

    if isinstance(result, list):
        if not result:
            return None
        value = result[0]
    else:
        value = result

    if isinstance(value, etree._Element):
        txt = "".join(value.itertext())
    else:
        txt = str(value)
    normalized = normalize_text(txt)
    return normalized or None


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", maxsplit=1)[1]
    return tag


def text_without_critical_notes(node: etree._Element) -> str:
    parts = node.xpath(
        ".//text()[not(ancestor::tei:note) and not(ancestor::tei:app) and not(ancestor::tei:rdg)]",
        namespaces=TEI_NS,
    )
    return normalize_text(" ".join(str(part) for part in parts))


def nearest_div_metadata(node: etree._Element) -> dict[str, str]:
    div = node.xpath("ancestor::tei:div[1]", namespaces=TEI_NS)
    if not div:
        return {}
    div_node = div[0]
    head = div_node.xpath("./tei:head[1]//text()", namespaces=TEI_NS)
    result = {
        "div_type": div_node.get("type", ""),
        "div_n": div_node.get("n", ""),
        "div_xml_id": div_node.get("{http://www.w3.org/XML/1998/namespace}id", ""),
        "div_head": normalize_text(" ".join(head)),
    }
    return {k: v for k, v in result.items() if v}


def drama_context(node: etree._Element) -> dict[str, str]:
    context: dict[str, str] = {}
    act = node.xpath("ancestor::tei:div[@type='act'][1]", namespaces=TEI_NS)
    scene = node.xpath("ancestor::tei:div[@type='scene'][1]", namespaces=TEI_NS)
    if act:
        act_node = act[0]
        act_head = normalize_text(" ".join(act_node.xpath("./tei:head[1]//text()", namespaces=TEI_NS)))
        if act_head:
            context["act_head"] = act_head
        if act_node.get("n"):
            context["act_n"] = act_node.get("n", "")
    if scene:
        scene_node = scene[0]
        scene_head = normalize_text(" ".join(scene_node.xpath("./tei:head[1]//text()", namespaces=TEI_NS)))
        if scene_head:
            context["scene_head"] = scene_head
        if scene_node.get("n"):
            context["scene_n"] = scene_node.get("n", "")
    return context


def select_snippet_nodes(genre: str, root: etree._Element) -> tuple[str, list[etree._Element]]:
    if genre == "Drama":
        nodes = root.xpath("//tei:text//*[local-name()='sp' or local-name()='hisSp']", namespaces=TEI_NS)
        return "speech", nodes
    if genre == "Dikt":
        nodes = root.xpath("//tei:text//tei:l", namespaces=TEI_NS)
        if nodes:
            return "line", nodes
    nodes = root.xpath("//tei:text//tei:p", namespaces=TEI_NS)
    return "paragraph", nodes


def build_snippets_for_file(xml_file: Path) -> list[dict[str, Any]]:
    rel = xml_file.relative_to(ROOT_DIR).as_posix()
    genre = xml_file.relative_to(ROOT_DIR).parts[0]
    raw_xml = xml_file.read_text(encoding="utf-8", errors="replace")
    cleaned_xml = clean_text_for_parse(raw_xml)
    root = etree.fromstring(cleaned_xml.encode("utf-8"))

    title = (
        first_text(root, "(//tei:titleStmt/tei:title[@type='main'])[1]")
        or first_text(root, "(//tei:titleStmt/tei:title)[1]")
        or xml_file.stem
    )
    edition_id = first_text(root, "string((//tei:editionStmt/tei:edition)[1]/@xml:id)") or ""
    doc_id = edition_id or xml_file.stem
    unique_doc_key = xml_file.stem

    snippet_type, nodes = select_snippet_nodes(genre, root)
    snippets: list[dict[str, Any]] = []

    for idx, node in enumerate(nodes, start=1):
        text = text_without_critical_notes(node)
        if not text:
            continue

        snippet: dict[str, Any] = {
            "snippet_id": f"{unique_doc_key}::{snippet_type}::{idx:05d}",
            "doc_id": doc_id,
            "title": title,
            "genre": genre,
            "source_file": rel,
            "snippet_type": snippet_type,
            "xml_id": node.get("{http://www.w3.org/XML/1998/namespace}id"),
            "text": text,
            "container": nearest_div_metadata(node),
        }

        if genre == "Drama" and local_name(node.tag) in {"sp", "hisSp"}:
            speaker_text = normalize_text(" ".join(node.xpath("./tei:speaker//text()", namespaces=TEI_NS)))
            if speaker_text:
                snippet["speaker"] = speaker_text
            who = node.get("who")
            if who:
                snippet["who"] = who
            snippet["drama_context"] = drama_context(node)

        snippets.append(snippet)

    return snippets


def write_snippets() -> None:
    xml_files = sorted(ROOT_DIR.rglob(XML_GLOB))
    if not xml_files:
        raise FileNotFoundError(f"No XML files found under {ROOT_DIR}")

    EXPORT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_snippets: list[dict[str, Any]] = []

    for xml_file in xml_files:
        all_snippets.extend(build_snippets_for_file(xml_file))

    with SNIPPETS_OUTPUT_FILE.open("w", encoding="utf-8") as fh:
        for snippet in all_snippets:
            fh.write(json.dumps(snippet, ensure_ascii=False) + "\n")

    by_genre: dict[str, int] = {}
    by_type: dict[str, int] = {}
    for snippet in all_snippets:
        by_genre[snippet["genre"]] = by_genre.get(snippet["genre"], 0) + 1
        by_type[snippet["snippet_type"]] = by_type.get(snippet["snippet_type"], 0) + 1

    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "root": ROOT_DIR.as_posix(),
        "source_file_count": len(xml_files),
        "snippet_count": len(all_snippets),
        "counts_by_genre": dict(sorted(by_genre.items())),
        "counts_by_snippet_type": dict(sorted(by_type.items())),
        "output_file": SNIPPETS_OUTPUT_FILE.as_posix(),
    }
    SNIPPETS_SUMMARY_FILE.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote snippets: {SNIPPETS_OUTPUT_FILE}")
    print(f"Wrote summary: {SNIPPETS_SUMMARY_FILE}")
    print(f"Total snippets: {len(all_snippets)}")


def iterate_snippets_jsonl() -> list[dict[str, Any]]:
    if not SNIPPETS_OUTPUT_FILE.exists():
        raise FileNotFoundError(f"Missing snippets export: {SNIPPETS_OUTPUT_FILE}. Run `extract` first.")
    snippets: list[dict[str, Any]] = []
    with SNIPPETS_OUTPUT_FILE.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            snippets.append(json.loads(line))
    return snippets


def build_metadata_profile(snippets: list[dict[str, Any]]) -> dict[str, Any]:
    by_genre: dict[str, dict[str, Any]] = {}
    for snippet in snippets:
        genre = snippet.get("genre", "Unknown")
        genre_state = by_genre.setdefault(
            genre,
            {
                "snippet_count": 0,
                "snippet_types": {},
                "fields": {},
                "container_fields": {},
                "drama_context_fields": {},
            },
        )
        genre_state["snippet_count"] += 1
        snippet_type = snippet.get("snippet_type", "unknown")
        genre_state["snippet_types"][snippet_type] = genre_state["snippet_types"].get(snippet_type, 0) + 1

        for field, value in snippet.items():
            if field in {"container", "drama_context"}:
                continue
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            genre_state["fields"][field] = genre_state["fields"].get(field, 0) + 1

        container = snippet.get("container") or {}
        if isinstance(container, dict):
            for field, value in container.items():
                if value is None:
                    continue
                if isinstance(value, str) and not value.strip():
                    continue
                genre_state["container_fields"][field] = genre_state["container_fields"].get(field, 0) + 1

        drama_context = snippet.get("drama_context") or {}
        if isinstance(drama_context, dict):
            for field, value in drama_context.items():
                if value is None:
                    continue
                if isinstance(value, str) and not value.strip():
                    continue
                genre_state["drama_context_fields"][field] = genre_state["drama_context_fields"].get(field, 0) + 1

    def summarize_field_counts(field_counts: dict[str, int], total: int) -> dict[str, dict[str, Any]]:
        return {
            field: {
                "count": count,
                "coverage": round(count / total, 4) if total else 0.0,
            }
            for field, count in sorted(field_counts.items())
        }

    per_genre: dict[str, Any] = {}
    for genre, state in sorted(by_genre.items()):
        total = state["snippet_count"]
        top_level = summarize_field_counts(state["fields"], total)
        container = summarize_field_counts(state["container_fields"], total)
        drama_context = summarize_field_counts(state["drama_context_fields"], total)

        required_top_level = sorted([k for k, v in top_level.items() if v["coverage"] >= 0.9999])
        optional_top_level = sorted([k for k, v in top_level.items() if 0 < v["coverage"] < 0.9999])

        per_genre[genre] = {
            "snippet_count": total,
            "snippet_types": dict(sorted(state["snippet_types"].items())),
            "recommended_profile": {
                "required_top_level_fields": required_top_level,
                "optional_top_level_fields": optional_top_level,
                "optional_container_fields": sorted(container.keys()),
                "optional_drama_context_fields": sorted(drama_context.keys()),
            },
            "field_coverage": {
                "top_level": top_level,
                "container": container,
                "drama_context": drama_context,
            },
        }

    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_file": SNIPPETS_OUTPUT_FILE.as_posix(),
        "total_snippets": len(snippets),
        "genres": per_genre,
    }


def write_metadata_profile() -> None:
    snippets = iterate_snippets_jsonl()
    profile = build_metadata_profile(snippets)
    EXPORT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_PROFILE_FILE.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote metadata profile: {METADATA_PROFILE_FILE}")
    print(f"Total snippets profiled: {profile['total_snippets']}")


def to_db_row(snippet: dict[str, Any]) -> dict[str, str | None]:
    container = snippet.get("container") or {}
    drama_context = snippet.get("drama_context") or {}
    return {
        "snippet_id": snippet.get("snippet_id"),
        "doc_id": snippet.get("doc_id"),
        "title": snippet.get("title"),
        "genre": snippet.get("genre"),
        "source_file": snippet.get("source_file"),
        "snippet_type": snippet.get("snippet_type"),
        "xml_id": snippet.get("xml_id"),
        "speaker": snippet.get("speaker"),
        "who": snippet.get("who"),
        "text": snippet.get("text"),
        "div_type": container.get("div_type"),
        "div_n": container.get("div_n"),
        "div_xml_id": container.get("div_xml_id"),
        "div_head": container.get("div_head"),
        "act_n": drama_context.get("act_n"),
        "act_head": drama_context.get("act_head"),
        "scene_n": drama_context.get("scene_n"),
        "scene_head": drama_context.get("scene_head"),
    }


def write_sqlite_index() -> None:
    snippets = iterate_snippets_jsonl()
    EXPORT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if INDEX_DB_FILE.exists():
        INDEX_DB_FILE.unlink()

    conn = sqlite3.connect(INDEX_DB_FILE)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute(
            """
            CREATE TABLE snippets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snippet_id TEXT UNIQUE NOT NULL,
                doc_id TEXT,
                title TEXT,
                genre TEXT,
                source_file TEXT,
                snippet_type TEXT,
                xml_id TEXT,
                speaker TEXT,
                who TEXT,
                text TEXT,
                div_type TEXT,
                div_n TEXT,
                div_xml_id TEXT,
                div_head TEXT,
                act_n TEXT,
                act_head TEXT,
                scene_n TEXT,
                scene_head TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE VIRTUAL TABLE snippets_fts USING fts5(
                snippet_id,
                speaker,
                text
            )
            """
        )

        rows = [to_db_row(snippet) for snippet in snippets]
        conn.executemany(
            """
            INSERT INTO snippets (
                snippet_id, doc_id, title, genre, source_file, snippet_type, xml_id, speaker, who, text,
                div_type, div_n, div_xml_id, div_head, act_n, act_head, scene_n, scene_head
            )
            VALUES (
                :snippet_id, :doc_id, :title, :genre, :source_file, :snippet_type, :xml_id, :speaker, :who, :text,
                :div_type, :div_n, :div_xml_id, :div_head, :act_n, :act_head, :scene_n, :scene_head
            )
            """,
            rows,
        )
        conn.executemany(
            """
            INSERT INTO snippets_fts (rowid, snippet_id, speaker, text)
            VALUES (?, ?, ?, ?)
            """,
            [
                (idx, row["snippet_id"], row["speaker"], row["text"])
                for idx, row in enumerate(rows, start=1)
            ],
        )
        conn.execute("CREATE INDEX idx_snippets_genre ON snippets(genre)")
        conn.execute("CREATE INDEX idx_snippets_type ON snippets(snippet_type)")
        conn.execute("CREATE INDEX idx_snippets_source_file ON snippets(source_file)")
        conn.execute("CREATE INDEX idx_snippets_speaker ON snippets(speaker)")
        conn.commit()
    finally:
        conn.close()

    print(f"Wrote sqlite index: {INDEX_DB_FILE}")
    print(f"Indexed snippets: {len(snippets)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="TEI utilities for manifest and snippet extraction.")
    parser.add_argument(
        "command",
        nargs="?",
        choices=("manifest", "extract", "profile", "index"),
        default="manifest",
        help="Which command to run. Default: manifest",
    )
    args = parser.parse_args()

    if args.command == "manifest":
        write_manifest()
    elif args.command == "extract":
        write_snippets()
    elif args.command == "profile":
        write_metadata_profile()
    elif args.command == "index":
        write_sqlite_index()


if __name__ == "__main__":
    main()
