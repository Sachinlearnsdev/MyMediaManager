#!/usr/bin/env python3
"""
bookpilot.py -- Metadata extractor for books and comics.
Reads files from Processing, extracts metadata from EPUB OPF, CBZ ComicInfo.xml,
PDF properties, and filenames, then writes .meta.json sidecars.

Supported formats:
  - EPUB: Dublin Core metadata from OPF (title, author, publisher, ISBN, genre, language)
  - CBZ/CBR: ComicInfo.xml (series, number, year, writer, publisher)
  - PDF: Document properties (title, author, subject)
  - All: Filename fallback parsing
"""

import os
import sys
import time
import json
import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree

_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import BOOK_EXTS, COMIC_EXTS, SCAN_INTERVAL
import common

log, CFG = common.setup_logger("bookpilot")

INPUT_DIR = Path(CFG['paths']['books_pipeline']['processing'])
FAILED_DIR = Path(CFG['paths']['books_pipeline']['failed'])

# Dublin Core namespace used in EPUB OPF files
DC_NS = "http://purl.org/dc/elements/1.1/"
OPF_NS = "http://www.idpf.org/2007/opf"


def _text(el):
    """Safely extract text from an XML element."""
    return el.text.strip() if el is not None and el.text else ""


def read_epub_metadata(file_path: Path) -> dict:
    """Extract metadata from an EPUB file's OPF document."""
    meta = {}
    try:
        with zipfile.ZipFile(str(file_path), 'r') as zf:
            # Find OPF file via container.xml
            opf_path = None
            try:
                container = zf.read("META-INF/container.xml")
                root = ElementTree.fromstring(container)
                ns = {"c": "urn:oasis:names:tc:opendocument:xmlns:container"}
                rootfile = root.find(".//c:rootfile", ns)
                if rootfile is not None:
                    opf_path = rootfile.get("full-path")
            except Exception:
                pass

            # Fallback: find any .opf file
            if not opf_path:
                for name in zf.namelist():
                    if name.endswith(".opf"):
                        opf_path = name
                        break

            if not opf_path:
                return meta

            opf_data = zf.read(opf_path)
            tree = ElementTree.fromstring(opf_data)

            # Dublin Core fields
            meta["title"] = _text(tree.find(f".//{{{DC_NS}}}title"))
            meta["author"] = _text(tree.find(f".//{{{DC_NS}}}creator"))
            meta["publisher"] = _text(tree.find(f".//{{{DC_NS}}}publisher"))
            meta["language"] = _text(tree.find(f".//{{{DC_NS}}}language"))
            meta["description"] = _text(tree.find(f".//{{{DC_NS}}}description"))
            meta["date"] = _text(tree.find(f".//{{{DC_NS}}}date"))

            # Subject/genre (can be multiple)
            subjects = tree.findall(f".//{{{DC_NS}}}subject")
            meta["subjects"] = [_text(s) for s in subjects if _text(s)]

            # ISBN from identifier elements
            identifiers = tree.findall(f".//{{{DC_NS}}}identifier")
            for ident in identifiers:
                text = _text(ident)
                scheme = ident.get(f"{{{OPF_NS}}}scheme", "").upper()
                if scheme == "ISBN" or re.match(r'^(?:97[89])?\d{9}[\dXx]$', text.replace("-", "")):
                    meta["isbn"] = text.replace("-", "")
                    break

            # Extract year from date
            if meta.get("date"):
                year_match = re.search(r'(\d{4})', meta["date"])
                if year_match:
                    meta["year"] = year_match.group(1)

    except (zipfile.BadZipFile, Exception) as e:
        log.debug(f"EPUB parse error for {file_path.name}: {e}")

    return meta


def read_comicinfo(file_path: Path) -> dict:
    """Extract metadata from ComicInfo.xml inside a CBZ/CBR archive."""
    meta = {}
    try:
        with zipfile.ZipFile(str(file_path), 'r') as zf:
            # Look for ComicInfo.xml (case-insensitive)
            ci_name = None
            for name in zf.namelist():
                if name.lower() == "comicinfo.xml":
                    ci_name = name
                    break

            if not ci_name:
                return meta

            ci_data = zf.read(ci_name)
            tree = ElementTree.fromstring(ci_data)

            meta["series"] = _text(tree.find("Series"))
            meta["number"] = _text(tree.find("Number"))
            meta["year"] = _text(tree.find("Year"))
            meta["writer"] = _text(tree.find("Writer"))
            meta["publisher"] = _text(tree.find("Publisher"))
            meta["title"] = _text(tree.find("Title"))
            meta["genre"] = _text(tree.find("Genre"))
            meta["volume"] = _text(tree.find("Volume"))
            meta["page_count"] = _text(tree.find("PageCount"))
            meta["language"] = _text(tree.find("LanguageISO"))
            meta["manga"] = _text(tree.find("Manga"))  # "Yes" / "YesAndRightToLeft"

            # Normalize number to int if possible
            if meta.get("number"):
                try:
                    meta["number"] = int(float(meta["number"]))
                except (ValueError, TypeError):
                    pass

    except (zipfile.BadZipFile, Exception) as e:
        log.debug(f"ComicInfo parse error for {file_path.name}: {e}")

    return meta


def read_pdf_metadata(file_path: Path) -> dict:
    """Extract basic metadata from PDF document properties.
    Uses a lightweight approach reading the PDF trailer without heavy dependencies."""
    meta = {}
    try:
        # Read first 4KB for quick metadata extraction
        with open(file_path, 'rb') as f:
            header = f.read(4096)

        # Verify it's a PDF
        if not header.startswith(b'%PDF'):
            return meta

        # Read last 4KB for trailer/metadata
        with open(file_path, 'rb') as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 8192))
            trailer = f.read()

        # Extract /Title, /Author, /Subject from info dict
        for tag, key in [(b'/Title', 'title'), (b'/Author', 'author'), (b'/Subject', 'subject')]:
            match = re.search(tag + rb'\s*\(([^)]*)\)', trailer)
            if not match:
                match = re.search(tag + rb'\s*\(([^)]*)\)', header)
            if match:
                try:
                    meta[key] = match.group(1).decode('utf-8', errors='replace').strip()
                except Exception:
                    pass

    except Exception as e:
        log.debug(f"PDF parse error for {file_path.name}: {e}")

    return meta


# ─── Web-download noise patterns ───
_BOOK_NOISE = [
    # Download site tags
    r'\(z-lib(?:\.org)?\)',
    r'\[z-lib(?:\.org)?\]',
    r'\b(?:z-lib|zlib|zlibrary|libgen|lib\.gen|b-ok|bookfi|sci-hub)\b',
    r'\((?:www\.)?[a-z0-9\-]+\.(?:com|org|net|io)\)',
    r'\[(?:www\.)?[a-z0-9\-]+\.(?:com|org|net|io)\]',
    # Format/quality tags already in extension
    r'\b(?:epub|mobi|pdf|azw3?|fb2|djvu|cbz|cbr|cb7)\b',
    # Torrent/release noise
    r'\b(?:retail|scan|digital|hd|hq|fixed|proper|repack|converted)\b',
    r'\[(?:epub|mobi|pdf|scan|digital|hd|fixed|proper|retail)[^\]]*\]',
    r'\((?:epub|mobi|pdf|scan|digital|hd|fixed|proper|retail)[^)]*\)',
    # Common web prefixes
    r'^(?:download|free\s+download|get)\s*[-:]\s*',
    # Edition noise (but keep edition info in parentheses)
    r'\b(?:OCR|DRM[- ]?free|unabridged)\b',
]
_BOOK_NOISE_RE = [re.compile(p, re.IGNORECASE) for p in _BOOK_NOISE]


def _clean_book_noise(s: str) -> str:
    """Strip web-download noise from a book filename."""
    for pat in _BOOK_NOISE_RE:
        s = pat.sub('', s)
    s = re.sub(r'\(\s*\)', '', s)
    s = re.sub(r'\[\s*\]', '', s)
    s = re.sub(r'\s{2,}', ' ', s)
    return s.strip(' -_|.')


def parse_filename(file_path: Path) -> dict:
    """Parse metadata from filename as a fallback.
    Handles patterns like:
      - Author - Title (Year).epub
      - Series 001 (2020).cbz
      - Title Vol.01.cbz
      - Author_Title_2020.pdf
      - Title (z-lib.org).epub  (web download noise)
    """
    meta = {}
    stem = file_path.stem

    # Strip web-download noise first
    stem = _clean_book_noise(stem)

    # Extract year in parentheses or at end
    year_match = re.search(r'\((\d{4})\)', stem)
    if year_match:
        meta["year"] = year_match.group(1)
        stem = stem[:year_match.start()].strip()
    else:
        year_match = re.search(r'[\._\s](\d{4})$', stem)
        if year_match:
            meta["year"] = year_match.group(1)
            stem = stem[:year_match.start()].strip()

    # Extract edition (e.g., "2nd Edition", "3rd Ed.")
    ed_match = re.search(r'\(?\b(\d+(?:st|nd|rd|th)\s+(?:edition|ed\.?))\b\)?', stem, re.I)
    if ed_match:
        meta["edition"] = ed_match.group(1).strip()
        stem = stem[:ed_match.start()] + stem[ed_match.end():]
        stem = stem.strip()

    # Extract volume/issue number
    vol_match = re.search(r'(?:Vol\.?|Volume|v)\s*(\d+)', stem, re.I)
    if vol_match:
        meta["volume"] = int(vol_match.group(1))

    issue_match = re.search(r'(?:#|No\.?|Issue)\s*(\d+)', stem, re.I)
    if issue_match:
        meta["number"] = int(issue_match.group(1))

    # Trailing number pattern: "Series 001"
    if not meta.get("number") and not meta.get("volume"):
        trailing = re.search(r'\s+(\d{1,4})$', stem)
        if trailing:
            meta["number"] = int(trailing.group(1))
            stem = stem[:trailing.start()].strip()

    # Author - Title split
    if " - " in stem:
        parts = stem.split(" - ", 1)
        meta["author"] = parts[0].strip()
        meta["title"] = parts[1].strip()
    elif " by " in stem.lower():
        # "Title by Author" pattern (common in web downloads)
        idx = stem.lower().index(" by ")
        meta["title"] = stem[:idx].strip()
        meta["author"] = stem[idx + 4:].strip()
    else:
        # Clean up underscores/dots as separators
        clean = re.sub(r'[_.]', ' ', stem).strip()
        # Remove volume/issue text for title
        clean = re.sub(r'\b(?:Vol\.?\s*\d+|Volume\s*\d+|#\d+|No\.?\s*\d+)\b', '', clean, flags=re.I).strip()
        meta["title"] = clean

    return meta


def extract_metadata(file_path: Path) -> dict:
    """Main metadata extraction: tries format-specific parsing, then falls back to filename."""
    ext = file_path.suffix.lower()
    meta = {"source_file": file_path.name, "format": ext}

    # Format-specific extraction
    if ext == ".epub":
        meta["epub"] = read_epub_metadata(file_path)
    elif ext in (".cbz", ".cb7"):
        meta["comicinfo"] = read_comicinfo(file_path)
    elif ext == ".cbr":
        # CBR is RAR — can't use zipfile, but try anyway (some are mis-named ZIPs)
        meta["comicinfo"] = read_comicinfo(file_path)
    elif ext == ".pdf":
        meta["pdf"] = read_pdf_metadata(file_path)

    # Filename fallback
    meta["filename_parsed"] = parse_filename(file_path)

    # Build unified tags from best available data
    tags = {}
    if ext == ".epub" and meta.get("epub"):
        ep = meta["epub"]
        tags["title"] = ep.get("title", "")
        tags["author"] = ep.get("author", "")
        tags["publisher"] = ep.get("publisher", "")
        tags["year"] = ep.get("year", "")
        tags["isbn"] = ep.get("isbn", "")
        tags["language"] = ep.get("language", "")
        tags["subjects"] = ep.get("subjects", [])
        tags["description"] = ep.get("description", "")
    elif ext in (".cbz", ".cbr", ".cb7", ".cbt") and meta.get("comicinfo"):
        ci = meta["comicinfo"]
        tags["series"] = ci.get("series", "")
        tags["title"] = ci.get("title", "")
        tags["number"] = ci.get("number", "")
        tags["volume"] = ci.get("volume", "")
        tags["year"] = ci.get("year", "")
        tags["writer"] = ci.get("writer", "")
        tags["publisher"] = ci.get("publisher", "")
        tags["genre"] = ci.get("genre", "")
        tags["manga"] = ci.get("manga", "")
        tags["page_count"] = ci.get("page_count", "")
    elif ext == ".pdf" and meta.get("pdf"):
        pdf = meta["pdf"]
        tags["title"] = pdf.get("title", "")
        tags["author"] = pdf.get("author", "")
        tags["subject"] = pdf.get("subject", "")

    # Fill gaps from filename
    fn = meta.get("filename_parsed", {})
    if not tags.get("title"):
        tags["title"] = fn.get("title", "")
    if not tags.get("author") and not tags.get("writer"):
        tags["author"] = fn.get("author", "")
    if not tags.get("year"):
        tags["year"] = fn.get("year", "")
    if not tags.get("number") and fn.get("number"):
        tags["number"] = fn.get("number", "")
    if not tags.get("volume") and fn.get("volume"):
        tags["volume"] = fn.get("volume", "")

    meta["tags"] = tags
    return meta


def write_meta(file_path: Path, meta: dict):
    """Write .meta.json sidecar next to the file."""
    meta_path = file_path.with_name(file_path.name + ".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding='utf-8')
    return meta_path


def main():
    for d in [INPUT_DIR, FAILED_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    log.info(f"Service Started. Watching: {INPUT_DIR}")

    all_book_exts = BOOK_EXTS | COMIC_EXTS

    while True:
        try:
            files = sorted(
                [f for f in INPUT_DIR.iterdir()
                 if f.is_file() and f.suffix.lower() in all_book_exts],
                key=lambda p: p.stat().st_mtime
            )
            for f in files:
                meta_path = f.with_name(f.name + ".meta.json")
                if meta_path.exists():
                    continue  # Already processed

                try:
                    meta = extract_metadata(f)
                    write_meta(f, meta)
                    log.info(f"META -> {f.name} (title={meta['tags'].get('title', '?')})")
                except Exception as e:
                    log.error(f"Failed to extract metadata from {f.name}: {e}")

        except Exception as e:
            log.error(f"Loop Error: {e}")

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
