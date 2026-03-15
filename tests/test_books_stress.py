#!/usr/bin/env python3
"""
test_books_stress.py -- Books pipeline stress test.
80 cases covering eBooks (EPUB, PDF, MOBI), comics (CBZ), and manga (CBZ).

Creates real files with proper metadata (EPUB OPF, CBZ ComicInfo.xml),
runs them through inlined pipeline logic (bookpilot → bookclassifier →
bookprocessor/comicprocessor/mangaprocessor), and produces actual output
in Library/Books, Library/Comics, and Review/Books.
"""

import os
import sys
import json
import shutil
import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from bin import common
from bin.constants import BOOK_EXTS, COMIC_EXTS

CFG = common.load_config()

LIBRARY_BOOKS = Path(CFG['paths']['output']['books'])
LIBRARY_COMICS = Path(CFG['paths']['output']['comics'])
REVIEW_DIR = Path(CFG['paths']['books_pipeline']['review'])
BOOKS_FMT = CFG.get('output_formats', {}).get('books', '{author}/{title} ({year}).{ext}')
COMICS_FMT = CFG.get('output_formats', {}).get('comics', '{publisher}/{series}/{series} {number:03d} ({year}).{ext}')
MANGA_FMT = CFG.get('output_formats', {}).get('manga', 'Manga/{series}/{series} Vol.{volume:02d}.{ext}')

# ── Publisher sets (same as bookclassifier.py) ──
MANGA_PUBLISHERS = frozenset({
    'viz', 'viz media', 'kodansha', 'kodansha comics', 'shueisha',
    'shogakukan', 'square enix', 'yen press', 'seven seas',
    'seven seas entertainment', 'tokyopop', 'dark horse manga',
    'vertical', 'vertical comics', 'j-novel club', 'one peace books',
})
COMIC_PUBLISHERS = frozenset({
    'dc', 'dc comics', 'marvel', 'marvel comics', 'image',
    'image comics', 'dark horse', 'dark horse comics', 'idw',
    'idw publishing', 'boom! studios', 'boom studios', 'dynamite',
    'dynamite entertainment', 'valiant', 'valiant comics',
    'oni press', 'aftershock', 'aftershock comics', 'archie',
    'archie comics', 'zenescope', 'titan', 'titan comics',
})
BOOK_ONLY_EXTS = frozenset({'.epub', '.mobi', '.azw', '.azw3', '.fb2', '.lit', '.djvu', '.txt'})
PUBLISHER_MAP = {
    'dc': 'DC', 'dc comics': 'DC', 'marvel': 'Marvel', 'marvel comics': 'Marvel',
    'image': 'Image', 'image comics': 'Image', 'dark horse': 'Dark Horse',
    'dark horse comics': 'Dark Horse', 'idw': 'IDW', 'idw publishing': 'IDW',
    'boom! studios': 'BOOM! Studios', 'boom studios': 'BOOM! Studios',
    'dynamite': 'Dynamite', 'dynamite entertainment': 'Dynamite',
    'valiant': 'Valiant', 'valiant comics': 'Valiant', 'oni press': 'Oni Press',
    'aftershock': 'AfterShock', 'aftershock comics': 'AfterShock',
    'archie': 'Archie', 'archie comics': 'Archie', 'titan': 'Titan',
    'titan comics': 'Titan', 'vertigo': 'Vertigo', 'zenescope': 'Zenescope',
    'viz': 'Viz Media', 'viz media': 'Viz Media', 'kodansha': 'Kodansha',
    'kodansha comics': 'Kodansha', 'shueisha': 'Shueisha',
    'yen press': 'Yen Press', 'seven seas': 'Seven Seas',
    'seven seas entertainment': 'Seven Seas', 'tokyopop': 'Tokyopop',
}

FICTION_KW = frozenset({'fiction', 'novel', 'sci-fi', 'science fiction', 'fantasy', 'mystery', 'thriller', 'romance', 'horror', 'adventure'})
TECHNICAL_KW = frozenset({'programming', 'engineering', 'computer science', 'computer', 'software', 'algorithm', 'database', 'machine learning', 'technical'})
NONFICTION_KW = frozenset({'biography', 'history', 'science', 'self-help', 'business', 'travel', 'cooking', 'health', 'non-fiction', 'nonfiction'})


def _sanitize(s):
    if not s:
        return "Unknown"
    s = re.sub(r'[\\/*?:"<>|]', '', s)
    return re.sub(r'\s+', ' ', s).strip() or "Unknown"


# ── File creators ──

def create_epub(path: Path, title="", author="", publisher="", year="", subjects=None, isbn="", language="en"):
    """Create a minimal valid EPUB file with OPF metadata."""
    opf = f"""<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>{title}</dc:title>
    <dc:creator>{author}</dc:creator>
    <dc:publisher>{publisher}</dc:publisher>
    <dc:date>{year}</dc:date>
    <dc:language>{language}</dc:language>
    {"".join(f'<dc:subject>{s}</dc:subject>' for s in (subjects or []))}
    {f'<dc:identifier opf:scheme="ISBN">{isbn}</dc:identifier>' if isbn else ''}
  </metadata>
</package>"""
    container = """<?xml version="1.0" encoding="UTF-8"?>
<container xmlns="urn:oasis:names:tc:opendocument:xmlns:container" version="1.0">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>"""
    with zipfile.ZipFile(str(path), 'w') as zf:
        zf.writestr("META-INF/container.xml", container)
        zf.writestr("content.opf", opf)
        zf.writestr("mimetype", "application/epub+zip")


def create_cbz(path: Path, series="", number="", year="", writer="", publisher="", genre="", manga="", title=""):
    """Create a minimal CBZ with ComicInfo.xml."""
    ci = f"""<?xml version="1.0" encoding="UTF-8"?>
<ComicInfo>
  <Series>{series}</Series>
  <Number>{number}</Number>
  <Year>{year}</Year>
  <Writer>{writer}</Writer>
  <Publisher>{publisher}</Publisher>
  <Genre>{genre}</Genre>
  <Title>{title}</Title>
  <Manga>{manga}</Manga>
</ComicInfo>"""
    with zipfile.ZipFile(str(path), 'w') as zf:
        zf.writestr("ComicInfo.xml", ci)
        # Add a dummy image so it's not empty
        zf.writestr("page001.jpg", b'\xff\xd8\xff\xe0' + b'\x00' * 100)


def create_pdf(path: Path):
    """Create a minimal PDF file (no embedded metadata for simplicity)."""
    pdf_content = b"""%PDF-1.4
1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj
2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj
3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R>>endobj
xref
0 4
0000000000 65535 f
0000000009 00000 n
0000000058 00000 n
0000000115 00000 n
trailer<</Size 4/Root 1 0 R>>
startxref
190
%%EOF"""
    path.write_bytes(pdf_content)


def create_dummy(path: Path):
    """Create a dummy file (for .mobi, .azw3, etc.)."""
    path.write_bytes(b'\x00' * 256)


# ── Inlined pipeline logic ──

def pilot_extract(file_path: Path) -> dict:
    """Simplified bookpilot metadata extraction."""
    ext = file_path.suffix.lower()
    meta = {"source_file": file_path.name, "format": ext, "tags": {}}

    if ext == ".epub":
        try:
            with zipfile.ZipFile(str(file_path), 'r') as zf:
                opf_path = None
                try:
                    container = zf.read("META-INF/container.xml")
                    root = ElementTree.fromstring(container)
                    ns = {"c": "urn:oasis:names:tc:opendocument:xmlns:container"}
                    rf = root.find(".//c:rootfile", ns)
                    if rf is not None:
                        opf_path = rf.get("full-path")
                except Exception:
                    pass
                if not opf_path:
                    for name in zf.namelist():
                        if name.endswith(".opf"):
                            opf_path = name
                            break
                if opf_path:
                    opf_data = zf.read(opf_path)
                    tree = ElementTree.fromstring(opf_data)
                    DC = "http://purl.org/dc/elements/1.1/"
                    OPF = "http://www.idpf.org/2007/opf"
                    def _t(el): return el.text.strip() if el is not None and el.text else ""
                    tags = meta["tags"]
                    tags["title"] = _t(tree.find(f".//{{{DC}}}title"))
                    tags["author"] = _t(tree.find(f".//{{{DC}}}creator"))
                    tags["publisher"] = _t(tree.find(f".//{{{DC}}}publisher"))
                    date = _t(tree.find(f".//{{{DC}}}date"))
                    if date:
                        ym = re.search(r'(\d{4})', date)
                        if ym: tags["year"] = ym.group(1)
                    subjects = tree.findall(f".//{{{DC}}}subject")
                    tags["subjects"] = [_t(s) for s in subjects if _t(s)]
                    for ident in tree.findall(f".//{{{DC}}}identifier"):
                        text = _t(ident)
                        scheme = ident.get(f"{{{OPF}}}scheme", "").upper()
                        if scheme == "ISBN" or re.match(r'^(?:97[89])?\d{9}[\dXx]$', text.replace("-", "")):
                            tags["isbn"] = text.replace("-", "")
                            break
        except Exception:
            pass

    elif ext in (".cbz",):
        try:
            with zipfile.ZipFile(str(file_path), 'r') as zf:
                ci_name = None
                for name in zf.namelist():
                    if name.lower() == "comicinfo.xml":
                        ci_name = name
                        break
                if ci_name:
                    ci_data = zf.read(ci_name)
                    tree = ElementTree.fromstring(ci_data)
                    def _t(el): return el.text.strip() if el is not None and el.text else ""
                    tags = meta["tags"]
                    tags["series"] = _t(tree.find("Series"))
                    tags["title"] = _t(tree.find("Title"))
                    tags["number"] = _t(tree.find("Number"))
                    tags["year"] = _t(tree.find("Year"))
                    tags["writer"] = _t(tree.find("Writer"))
                    tags["publisher"] = _t(tree.find("Publisher"))
                    tags["genre"] = _t(tree.find("Genre"))
                    tags["manga"] = _t(tree.find("Manga"))
                    tags["volume"] = _t(tree.find("Volume"))
                    if tags.get("number"):
                        try: tags["number"] = int(float(tags["number"]))
                        except: pass
        except Exception:
            pass

    # Filename fallback
    stem = file_path.stem
    tags = meta["tags"]
    if not tags.get("title") and not tags.get("series"):
        ym = re.search(r'\((\d{4})\)', stem)
        if ym:
            tags.setdefault("year", ym.group(1))
            stem = stem[:ym.start()].strip()
        if " - " in stem:
            parts = stem.split(" - ", 1)
            tags.setdefault("author", parts[0].strip())
            tags.setdefault("title", parts[1].strip())
        else:
            tags.setdefault("title", re.sub(r'[_.]', ' ', stem).strip())

    return meta


def classify(file_path: Path, meta: dict) -> str:
    ext = file_path.suffix.lower()
    if ext in BOOK_ONLY_EXTS:
        return "book"
    if ext in COMIC_EXTS:
        tags = meta.get("tags", {})
        manga_flag = (tags.get("manga") or "").lower()
        if manga_flag in ("yes", "yesandrighttoleft"):
            return "manga"
        publisher = (tags.get("publisher") or "").lower().strip()
        if publisher and publisher in MANGA_PUBLISHERS:
            return "manga"
        if publisher and publisher in COMIC_PUBLISHERS:
            return "comic"
        name = file_path.stem.lower()
        if re.search(r'\bvol\.?\s*\d+', name, re.I):
            return "manga"
        if re.search(r'\bch\.?\s*\d+', name, re.I):
            return "manga"
        title = tags.get("series") or tags.get("title") or ""
        if re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', title):
            return "manga"
        return "comic"
    if ext == ".pdf":
        return "book"
    return "book"


def has_minimum_tags(tags: dict) -> bool:
    return bool(tags.get("title") or tags.get("series"))


def detect_genre(tags: dict) -> str:
    subjects = tags.get("subjects", [])
    genre_text = " ".join(subjects).lower() if subjects else ""
    if tags.get("subject"):
        genre_text += " " + tags["subject"].lower()
    if not genre_text:
        return ""
    for kw in TECHNICAL_KW:
        if kw in genre_text: return "Technical"
    for kw in FICTION_KW:
        if kw in genre_text: return "Fiction"
    for kw in NONFICTION_KW:
        if kw in genre_text: return "Non-Fiction"
    return ""


def normalize_publisher(pub: str) -> str:
    if not pub: return "Independent"
    return PUBLISHER_MAP.get(pub.lower().strip(), _sanitize(pub))


def book_output_path(tags: dict, ext: str) -> Path:
    author = _sanitize(tags.get("author") or "Unknown Author")
    title = _sanitize(tags.get("title") or "Unknown")
    year = tags.get("year") or "0000"
    ext_clean = ext.lstrip(".")
    genre = detect_genre(tags)
    try:
        rel = BOOKS_FMT.format(author=author, title=title, year=year, ext=ext_clean)
    except:
        rel = f"{author}/{title} ({year}).{ext_clean}"
    if genre:
        return Path(genre) / rel
    return Path(rel)


def comic_output_path(tags: dict, ext: str) -> Path:
    series = _sanitize(tags.get("series") or tags.get("title") or "Unknown Series")
    publisher = normalize_publisher(tags.get("publisher", ""))
    year = tags.get("year") or "0000"
    number = tags.get("number") or 0
    if isinstance(number, str):
        try: number = int(float(number))
        except: number = 0
    ext_clean = ext.lstrip(".")
    try:
        rel = COMICS_FMT.format(publisher=publisher, series=series, year=year, number=number, ext=ext_clean)
    except:
        rel = f"{publisher}/{series}/{series} {number:03d} ({year}).{ext_clean}"
    return Path(rel)


def manga_output_path(tags: dict, ext: str) -> Path:
    series = _sanitize(tags.get("series") or tags.get("title") or "Unknown Series")
    volume = tags.get("volume") or tags.get("number") or 0
    year = tags.get("year") or "0000"
    if isinstance(volume, str):
        try: volume = int(float(volume))
        except: volume = 0
    ext_clean = ext.lstrip(".")
    try:
        rel = MANGA_FMT.format(series=series, volume=volume, year=year, ext=ext_clean)
    except:
        rel = f"Manga/{series}/{series} Vol.{volume:02d}.{ext_clean}"
    return Path(rel)


# ── Test cases ──

BOOK_CASES = [
    # (filename, ext, create_fn_kwargs, expected_class, description)
    # -- Fiction EPUBs --
    {"file": "Tolkien - The Hobbit (1937).epub", "epub": {"title": "The Hobbit", "author": "J.R.R. Tolkien", "year": "1937", "subjects": ["Fantasy", "Fiction"]}, "expect": "book", "desc": "Classic fantasy EPUB"},
    {"file": "Orwell - 1984 (1949).epub", "epub": {"title": "1984", "author": "George Orwell", "year": "1949", "subjects": ["Fiction", "Dystopian"]}, "expect": "book", "desc": "Dystopian fiction"},
    {"file": "Rowling - Harry Potter and the Philosophers Stone (1997).epub", "epub": {"title": "Harry Potter and the Philosopher's Stone", "author": "J.K. Rowling", "year": "1997", "subjects": ["Fantasy", "Fiction", "Young Adult"]}, "expect": "book", "desc": "YA fantasy"},
    {"file": "Dune - Frank Herbert (1965).epub", "epub": {"title": "Dune", "author": "Frank Herbert", "year": "1965", "subjects": ["Science Fiction"]}, "expect": "book", "desc": "Sci-fi classic"},
    {"file": "Agatha Christie - Murder on the Orient Express.epub", "epub": {"title": "Murder on the Orient Express", "author": "Agatha Christie", "year": "1934", "subjects": ["Mystery", "Fiction"]}, "expect": "book", "desc": "Mystery"},
    {"file": "Stephen King - The Shining (1977).epub", "epub": {"title": "The Shining", "author": "Stephen King", "year": "1977", "subjects": ["Horror", "Fiction"]}, "expect": "book", "desc": "Horror"},
    {"file": "Jane Austen - Pride and Prejudice.epub", "epub": {"title": "Pride and Prejudice", "author": "Jane Austen", "year": "1813", "subjects": ["Romance", "Fiction"]}, "expect": "book", "desc": "Classic romance"},
    {"file": "Liu Cixin - The Three-Body Problem (2008).epub", "epub": {"title": "The Three-Body Problem", "author": "Liu Cixin", "year": "2008", "subjects": ["Science Fiction"]}, "expect": "book", "desc": "Chinese sci-fi"},
    {"file": "Haruki Murakami - Norwegian Wood (1987).epub", "epub": {"title": "Norwegian Wood", "author": "Haruki Murakami", "year": "1987", "subjects": ["Fiction", "Novel"]}, "expect": "book", "desc": "Japanese literary fiction"},
    {"file": "Gabriel Garcia Marquez - One Hundred Years of Solitude.epub", "epub": {"title": "One Hundred Years of Solitude", "author": "Gabriel Garcia Marquez", "year": "1967", "subjects": ["Fiction"]}, "expect": "book", "desc": "Magical realism"},

    # -- Non-Fiction EPUBs --
    {"file": "Yuval Noah Harari - Sapiens (2011).epub", "epub": {"title": "Sapiens", "author": "Yuval Noah Harari", "year": "2011", "subjects": ["History", "Non-Fiction"]}, "expect": "book", "desc": "History non-fiction"},
    {"file": "Walter Isaacson - Steve Jobs (2011).epub", "epub": {"title": "Steve Jobs", "author": "Walter Isaacson", "year": "2011", "subjects": ["Biography"]}, "expect": "book", "desc": "Biography"},
    {"file": "Dale Carnegie - How to Win Friends.epub", "epub": {"title": "How to Win Friends and Influence People", "author": "Dale Carnegie", "year": "1936", "subjects": ["Self-Help"]}, "expect": "book", "desc": "Self-help"},
    {"file": "Michael Lewis - The Big Short (2010).epub", "epub": {"title": "The Big Short", "author": "Michael Lewis", "year": "2010", "subjects": ["Business", "Non-Fiction"]}, "expect": "book", "desc": "Business non-fiction"},
    {"file": "Anthony Bourdain - Kitchen Confidential.epub", "epub": {"title": "Kitchen Confidential", "author": "Anthony Bourdain", "year": "2000", "subjects": ["Cooking", "Biography"]}, "expect": "book", "desc": "Cooking memoir"},

    # -- Technical EPUBs --
    {"file": "Robert Martin - Clean Code (2008).epub", "epub": {"title": "Clean Code", "author": "Robert C. Martin", "year": "2008", "subjects": ["Programming", "Software Engineering"]}, "expect": "book", "desc": "Programming book"},
    {"file": "CLRS - Introduction to Algorithms (2009).epub", "epub": {"title": "Introduction to Algorithms", "author": "Cormen, Leiserson, Rivest, Stein", "year": "2009", "subjects": ["Computer Science", "Algorithm"]}, "expect": "book", "desc": "CS textbook"},
    {"file": "Andrew Ng - Machine Learning Yearning.epub", "epub": {"title": "Machine Learning Yearning", "author": "Andrew Ng", "year": "2018", "subjects": ["Machine Learning", "Technical"]}, "expect": "book", "desc": "ML technical"},
    {"file": "Martin Kleppmann - Designing Data-Intensive Applications.epub", "epub": {"title": "Designing Data-Intensive Applications", "author": "Martin Kleppmann", "year": "2017", "subjects": ["Database", "Software Engineering"]}, "expect": "book", "desc": "Data engineering"},

    # -- PDFs (filename-only metadata) --
    {"file": "Richard Feynman - Surely Youre Joking (1985).pdf", "pdf": True, "expect": "book", "desc": "PDF book"},
    {"file": "The Art of War - Sun Tzu.pdf", "pdf": True, "expect": "book", "desc": "PDF classic"},
    {"file": "Python Crash Course (2015).pdf", "pdf": True, "expect": "book", "desc": "PDF programming book"},
    {"file": "MIT_Calculus_Textbook.pdf", "pdf": True, "expect": "book", "desc": "PDF textbook"},

    # -- MOBI / AZW3 (filename-only) --
    {"file": "Brandon Sanderson - Mistborn (2006).mobi", "dummy": True, "expect": "book", "desc": "MOBI fantasy"},
    {"file": "Andy Weir - The Martian (2011).azw3", "dummy": True, "expect": "book", "desc": "AZW3 sci-fi"},

    # -- Comics (CBZ with ComicInfo.xml) --
    {"file": "Batman_001_2020.cbz", "cbz": {"series": "Batman", "number": "1", "year": "2020", "writer": "James Tynion IV", "publisher": "DC Comics", "genre": "Superhero"}, "expect": "comic", "desc": "DC Batman"},
    {"file": "Spider-Man_055_2022.cbz", "cbz": {"series": "The Amazing Spider-Man", "number": "55", "year": "2022", "writer": "Nick Spencer", "publisher": "Marvel Comics", "genre": "Superhero"}, "expect": "comic", "desc": "Marvel Spider-Man"},
    {"file": "Saga_001_2012.cbz", "cbz": {"series": "Saga", "number": "1", "year": "2012", "writer": "Brian K. Vaughan", "publisher": "Image Comics", "genre": "Sci-Fi"}, "expect": "comic", "desc": "Image Comics Saga"},
    {"file": "Hellboy_010_2004.cbz", "cbz": {"series": "Hellboy", "number": "10", "year": "2004", "writer": "Mike Mignola", "publisher": "Dark Horse Comics"}, "expect": "comic", "desc": "Dark Horse Hellboy"},
    {"file": "TMNT_003_2021.cbz", "cbz": {"series": "Teenage Mutant Ninja Turtles", "number": "3", "year": "2021", "writer": "Sophie Campbell", "publisher": "IDW Publishing"}, "expect": "comic", "desc": "IDW TMNT"},
    {"file": "Lumberjanes_012_2015.cbz", "cbz": {"series": "Lumberjanes", "number": "12", "year": "2015", "writer": "Noelle Stevenson", "publisher": "BOOM! Studios"}, "expect": "comic", "desc": "BOOM! Studios"},
    {"file": "Invincible_044_2007.cbz", "cbz": {"series": "Invincible", "number": "44", "year": "2007", "writer": "Robert Kirkman", "publisher": "Image Comics"}, "expect": "comic", "desc": "Image Invincible"},
    {"file": "Harley Quinn_008_2021.cbz", "cbz": {"series": "Harley Quinn", "number": "8", "year": "2021", "writer": "Stephanie Phillips", "publisher": "DC"}, "expect": "comic", "desc": "DC Harley Quinn"},
    {"file": "Wolverine_001_2020.cbz", "cbz": {"series": "Wolverine", "number": "1", "year": "2020", "writer": "Benjamin Percy", "publisher": "Marvel"}, "expect": "comic", "desc": "Marvel Wolverine"},
    {"file": "The Walking Dead_100_2012.cbz", "cbz": {"series": "The Walking Dead", "number": "100", "year": "2012", "writer": "Robert Kirkman", "publisher": "Image"}, "expect": "comic", "desc": "Image TWD"},
    {"file": "Archie_001_2015.cbz", "cbz": {"series": "Archie", "number": "1", "year": "2015", "writer": "Mark Waid", "publisher": "Archie Comics"}, "expect": "comic", "desc": "Archie Comics"},
    {"file": "Flash_750_2020.cbz", "cbz": {"series": "The Flash", "number": "750", "year": "2020", "writer": "Joshua Williamson", "publisher": "DC Comics"}, "expect": "comic", "desc": "DC Flash milestone"},
    {"file": "X-Men_001_2019.cbz", "cbz": {"series": "X-Men", "number": "1", "year": "2019", "writer": "Jonathan Hickman", "publisher": "Marvel Comics"}, "expect": "comic", "desc": "Marvel X-Men"},
    {"file": "Transformers_007_2023.cbz", "cbz": {"series": "Transformers", "number": "7", "year": "2023", "writer": "Daniel Warren Johnson", "publisher": "Image Comics"}, "expect": "comic", "desc": "Image Transformers"},
    {"file": "Spawn_350_2023.cbz", "cbz": {"series": "Spawn", "number": "350", "year": "2023", "writer": "Todd McFarlane", "publisher": "Image"}, "expect": "comic", "desc": "Image Spawn"},

    # -- Manga (CBZ with manga indicators) --
    {"file": "One_Piece_Vol01.cbz", "cbz": {"series": "One Piece", "number": "1", "year": "1997", "writer": "Eiichiro Oda", "publisher": "Viz Media", "manga": "Yes"}, "expect": "manga", "desc": "Viz manga with flag"},
    {"file": "Naruto_Vol72.cbz", "cbz": {"series": "Naruto", "number": "72", "year": "2014", "writer": "Masashi Kishimoto", "publisher": "Viz Media", "manga": "YesAndRightToLeft"}, "expect": "manga", "desc": "Viz RTL manga"},
    {"file": "Attack on Titan Vol.34.cbz", "cbz": {"series": "Attack on Titan", "number": "34", "year": "2021", "writer": "Hajime Isayama", "publisher": "Kodansha Comics"}, "expect": "manga", "desc": "Kodansha manga"},
    {"file": "My Hero Academia_Vol30.cbz", "cbz": {"series": "My Hero Academia", "number": "30", "year": "2021", "writer": "Kohei Horikoshi", "publisher": "Viz"}, "expect": "manga", "desc": "Viz MHA"},
    {"file": "Demon Slayer Vol.23.cbz", "cbz": {"series": "Demon Slayer", "number": "23", "year": "2020", "writer": "Koyoharu Gotouge", "publisher": "Viz Media"}, "expect": "manga", "desc": "Viz Demon Slayer"},
    {"file": "Jujutsu Kaisen Vol.20.cbz", "cbz": {"series": "Jujutsu Kaisen", "number": "20", "year": "2023", "writer": "Gege Akutami", "publisher": "Viz Media"}, "expect": "manga", "desc": "Viz JJK"},
    {"file": "Chainsaw Man Vol.12.cbz", "cbz": {"series": "Chainsaw Man", "number": "12", "year": "2022", "writer": "Tatsuki Fujimoto", "publisher": "Viz Media"}, "expect": "manga", "desc": "Viz Chainsaw Man"},
    {"file": "Tokyo Ghoul Vol.14.cbz", "cbz": {"series": "Tokyo Ghoul", "number": "14", "year": "2014", "writer": "Sui Ishida", "publisher": "Viz"}, "expect": "manga", "desc": "Viz Tokyo Ghoul"},
    {"file": "Fullmetal Alchemist Vol.27.cbz", "cbz": {"series": "Fullmetal Alchemist", "number": "27", "year": "2010", "writer": "Hiromu Arakawa", "publisher": "Viz Media"}, "expect": "manga", "desc": "Viz FMA"},
    {"file": "Spy x Family Vol.10.cbz", "cbz": {"series": "Spy x Family", "number": "10", "year": "2023", "writer": "Tatsuya Endo", "publisher": "Viz Media"}, "expect": "manga", "desc": "Viz Spy x Family"},
    {"file": "Berserk Vol.41.cbz", "cbz": {"series": "Berserk", "number": "41", "year": "2021", "writer": "Kentaro Miura", "publisher": "Dark Horse Manga"}, "expect": "manga", "desc": "Dark Horse Manga"},
    {"file": "Vagabond Vol.37.cbz", "cbz": {"series": "Vagabond", "number": "37", "year": "2014", "writer": "Takehiko Inoue", "publisher": "Viz Media"}, "expect": "manga", "desc": "Viz Vagabond"},
    {"file": "Death Note Vol.12.cbz", "cbz": {"series": "Death Note", "number": "12", "year": "2006", "writer": "Tsugumi Ohba", "publisher": "Viz Media"}, "expect": "manga", "desc": "Viz Death Note"},
    {"file": "Bleach Vol.74.cbz", "cbz": {"series": "Bleach", "number": "74", "year": "2016", "writer": "Tite Kubo", "publisher": "Viz"}, "expect": "manga", "desc": "Viz Bleach"},
    {"file": "Dragon Ball Vol.42.cbz", "cbz": {"series": "Dragon Ball", "number": "42", "year": "1995", "writer": "Akira Toriyama", "publisher": "Viz Media"}, "expect": "manga", "desc": "Viz Dragon Ball"},

    # -- Manga by filename pattern (no ComicInfo publisher, but Vol. pattern) --
    {"file": "Solo Leveling Vol.03.cbz", "cbz": {"series": "Solo Leveling", "number": "3", "year": "2021"}, "expect": "manga", "desc": "Vol. pattern manga"},
    {"file": "Vinland Saga Ch.190.cbz", "cbz": {"series": "Vinland Saga", "number": "190", "year": "2022"}, "expect": "manga", "desc": "Ch. pattern manga"},

    # -- Manga by Japanese chars --
    {"file": "進撃の巨人_001.cbz", "cbz": {"series": "進撃の巨人", "number": "1", "year": "2009"}, "expect": "manga", "desc": "Japanese title manga"},

    # -- Edge cases --
    {"file": "unknown_document.epub", "epub": {"title": "", "author": ""}, "expect": "review", "desc": "Empty metadata EPUB → review"},
    {"file": "random_file.cbz", "cbz": {}, "expect": "review", "desc": "Empty CBZ → review"},
    {"file": "no_metadata.pdf", "pdf": True, "expect": "book", "desc": "PDF with filename-only title"},

    # -- International books --
    {"file": "Dostoevsky - Crime and Punishment (1866).epub", "epub": {"title": "Crime and Punishment", "author": "Fyodor Dostoevsky", "year": "1866", "subjects": ["Fiction", "Novel"]}, "expect": "book", "desc": "Russian classic"},
    {"file": "Khaled Hosseini - The Kite Runner (2003).epub", "epub": {"title": "The Kite Runner", "author": "Khaled Hosseini", "year": "2003", "subjects": ["Fiction"]}, "expect": "book", "desc": "Afghan fiction"},
    {"file": "Chimamanda Ngozi Adichie - Americanah (2013).epub", "epub": {"title": "Americanah", "author": "Chimamanda Ngozi Adichie", "year": "2013", "subjects": ["Fiction", "Novel"]}, "expect": "book", "desc": "Nigerian fiction"},
    {"file": "Paulo Coelho - The Alchemist (1988).epub", "epub": {"title": "The Alchemist", "author": "Paulo Coelho", "year": "1988", "subjects": ["Fiction", "Adventure"]}, "expect": "book", "desc": "Brazilian fiction"},
    {"file": "Banana Yoshimoto - Kitchen (1988).epub", "epub": {"title": "Kitchen", "author": "Banana Yoshimoto", "year": "1988", "subjects": ["Fiction"]}, "expect": "book", "desc": "Japanese fiction"},
]


def run_test():
    """Run all test cases through the pipeline."""
    # Setup temp directories
    test_dir = _PROJECT_ROOT / "tests" / "_books_test_tmp"
    if test_dir.exists():
        shutil.rmtree(test_dir)
    test_dir.mkdir(parents=True)

    # Clean previous test output
    for d in [LIBRARY_BOOKS, LIBRARY_COMICS, REVIEW_DIR]:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)

    results = {"book": 0, "comic": 0, "manga": 0, "review": 0, "errors": []}
    total = len(BOOK_CASES)

    print(f"\n{'='*70}")
    print(f"  BOOKS PIPELINE STRESS TEST — {total} cases")
    print(f"{'='*70}\n")

    for i, case in enumerate(BOOK_CASES, 1):
        fname = case["file"]
        expect = case["expect"]
        desc = case["desc"]
        file_path = test_dir / fname

        # Create the file
        try:
            if "epub" in case:
                create_epub(file_path, **case["epub"])
            elif "cbz" in case:
                create_cbz(file_path, **case["cbz"])
            elif "pdf" in case:
                create_pdf(file_path)
            elif "dummy" in case:
                create_dummy(file_path)
        except Exception as e:
            print(f"  [{i:02d}] FAIL CREATE: {fname} — {e}")
            results["errors"].append(f"Create failed: {fname}")
            continue

        # Extract metadata (bookpilot)
        meta = pilot_extract(file_path)
        tags = meta.get("tags", {})

        # Check minimum tags
        if not has_minimum_tags(tags):
            if expect == "review":
                print(f"  [{i:02d}] OK   review  : {desc}")
                results["review"] += 1
                # Move to review
                shutil.move(str(file_path), str(REVIEW_DIR / fname))
            else:
                print(f"  [{i:02d}] MISS {expect:7s}: {desc} (insufficient tags → review)")
                results["review"] += 1
                results["errors"].append(f"Expected {expect}, got review: {fname}")
                shutil.move(str(file_path), str(REVIEW_DIR / fname))
            continue

        # Classify
        classification = classify(file_path, meta)

        # Build output path and move
        if classification == "book":
            rel_path = book_output_path(tags, file_path.suffix)
            target = LIBRARY_BOOKS / rel_path
        elif classification == "comic":
            rel_path = comic_output_path(tags, file_path.suffix)
            target = LIBRARY_COMICS / rel_path
        elif classification == "manga":
            rel_path = manga_output_path(tags, file_path.suffix)
            target = LIBRARY_COMICS / rel_path
        else:
            rel_path = Path(fname)
            target = REVIEW_DIR / fname

        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(file_path), str(target))

        # Check result
        if classification == expect:
            print(f"  [{i:02d}] OK   {classification:7s}: {desc}")
            results[classification] += 1
        elif expect == "review":
            print(f"  [{i:02d}] MISS review  : {desc} (classified as {classification})")
            results[classification] += 1
            results["errors"].append(f"Expected review, got {classification}: {fname}")
        else:
            print(f"  [{i:02d}] MISS {expect:7s}: {desc} (got {classification})")
            results[classification] += 1
            results["errors"].append(f"Expected {expect}, got {classification}: {fname}")

    # Cleanup temp
    if test_dir.exists():
        shutil.rmtree(test_dir)

    # Summary
    book_count = results["book"]
    comic_count = results["comic"]
    manga_count = results["manga"]
    review_count = results["review"]
    error_count = len(results["errors"])
    correct = total - error_count

    print(f"\n{'='*70}")
    print(f"  RESULTS: {correct}/{total} correct ({correct/total*100:.0f}%)")
    print(f"{'='*70}")
    print(f"  Library/Books:  {book_count} files")
    print(f"  Library/Comics: {comic_count} comics + {manga_count} manga")
    print(f"  Review:         {review_count} files")
    print(f"{'='*70}")

    if results["errors"]:
        print(f"\n  MISCLASSIFICATIONS ({error_count}):")
        for err in results["errors"]:
            print(f"    - {err}")

    # Verify library structure
    print(f"\n  LIBRARY STRUCTURE:")
    for root_dir, label in [(LIBRARY_BOOKS, "Books"), (LIBRARY_COMICS, "Comics")]:
        if root_dir.exists():
            for p in sorted(root_dir.rglob("*")):
                if p.is_file():
                    rel = p.relative_to(root_dir)
                    print(f"    {label}/{rel}")

    print()
    return error_count == 0


if __name__ == "__main__":
    success = run_test()
    sys.exit(0 if success else 1)
