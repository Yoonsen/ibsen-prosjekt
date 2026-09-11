import os
import glob
import sqlite3
import copy
from lxml import etree

def clean_drama_sp(sp_element, ns):
    """Fjerner <speaker> og <stage> og returnerer ren tekst."""
    sp_copy = copy.deepcopy(sp_element)
    for bad in sp_copy.xpath('.//tei:speaker | .//tei:stage | .//his:hisStage', namespaces=ns):
        bad.getparent().remove(bad)
    # Samle all gjenværende tekst
    text = ' '.join(''.join(sp_copy.itertext()).split())
    return text

def main():
    db_path = "api/exports/tei_snippets_clean.db"
    
    # Slett gammel hvis den eksisterer
    if os.path.exists(db_path):
        os.remove(db_path)
        
    conn = sqlite3.connect(db_path)
    conn.execute('''
        CREATE TABLE snippets (
            snippet_id TEXT PRIMARY KEY,
            title TEXT,
            doc_id TEXT,
            source_file TEXT,
            genre TEXT,
            snippet_type TEXT,
            text TEXT
        )
    ''')
    
    ns = {'tei': 'http://www.tei-c.org/ns/1.0', 'his': 'http://www.example.org/ns/HIS'}
    # Vi skrur på recover=True for å ignorere ukjente entities (som &funder;) 
    # ettersom den gamle DTD-serveren til UiO/Ibsensentret noen ganger er nede.
    parser = etree.XMLParser(recover=True, resolve_entities=False, load_dtd=False, no_network=True)

    print("Prosesserer Drama (fjerner speaker/stage)...")
    for filepath in glob.glob("Ibsen-xml/Drama/*.xml"):
        filename = os.path.basename(filepath)
        doc_id = filename.replace('.xml', '')
        
        try:
            tree = etree.parse(filepath, parser)
        except Exception as e:
            print(f"Feil ved lesing av {filename}: {e}")
            continue
            
        title_elem = tree.find('.//tei:titleStmt/tei:title', namespaces=ns)
        title = title_elem.text if title_elem is not None else doc_id
        
        speeches = tree.xpath('//tei:sp | //his:hisSp', namespaces=ns)
        for i, sp in enumerate(speeches):
            clean_text = clean_drama_sp(sp, ns)
            if clean_text:
                snippet_id = f"{doc_id}::speech::{i+1:05d}"
                conn.execute(
                    "INSERT INTO snippets VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (snippet_id, title, doc_id, filepath, "Drama", "speech", clean_text)
                )

    print("Prosesserer Dikt (deler opp i enkeltdikt med egne titler)...")
    for filepath in glob.glob("Ibsen-xml/Dikt/*.xml"):
        filename = os.path.basename(filepath)
        doc_id = filename.replace('.xml', '')
        
        try:
            tree = etree.parse(filepath, parser)
        except Exception as e:
            continue
            
        # Finn alle dikt
        poems = tree.xpath('//tei:div[@type="poem"]', namespaces=ns)
        for poem_i, poem in enumerate(poems):
            head = poem.find('.//tei:head', namespaces=ns)
            if head is not None:
                poem_title = ' '.join(''.join(head.itertext()).split())
            else:
                poem_title = f"{doc_id} - Dikt {poem_i+1}"
            
            # Hent alle linjer i diktet
            lines = poem.xpath('.//tei:l', namespaces=ns)
            for l_i, l in enumerate(lines):
                text = ''.join(l.itertext()).strip()
                if text:
                    snippet_id = f"{doc_id}::poem::{poem_i+1:03d}::line::{l_i+1:05d}"
                    conn.execute(
                        "INSERT INTO snippets VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (snippet_id, poem_title, doc_id, filepath, "Dikt", "line", text)
                    )

    conn.commit()
    conn.close()
    print(f"Ferdig! Ren database lagret til {db_path}")

if __name__ == '__main__':
    main()
