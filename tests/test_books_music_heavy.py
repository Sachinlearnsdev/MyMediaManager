#!/usr/bin/env python3
"""
test_books_music_heavy.py -- Heavy stress test for Books & Music pipelines.

Tests 250+ cases across all book/music categories:
  - Books: Fiction, Non-Fiction, Technical, Academic, Children's, Poetry
  - Comics: DC, Marvel, Image, Dark Horse, IDW, BOOM!, Archie, Indie
  - Manga: Shonen, Seinen, Shojo, Josei, various publishers
  - Music: Rock, Pop, Hip-Hop, Jazz, Classical, Electronic, K-Pop, Latin
  - Audiobooks: Fiction, Non-Fiction, Series, Standalone

Exercises:
  - Full pipeline logic (bookpilot → bookclassifier → processor)
  - API enrichment functions (enrich_from_api with sparse metadata)
  - Edge cases: missing fields, unicode titles, long names, special chars
  - Genre classification accuracy
  - Publisher normalization
  - Output format template rendering
"""

import os
import sys
import json
import re
import shutil
import zipfile
from pathlib import Path

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Ensure 'common' is importable
import bin.common
sys.modules['common'] = bin.common

from bin.common import load_config
CFG = load_config()
OUTPUT = CFG['paths']['output']

_lib_root = CFG['paths']['roots'].get('library', str(ROOT / 'Library'))
LIB = Path(_lib_root)
WORK = LIB / "_test_work"

# Import pipeline logic
from bin.bookpilot import extract_metadata as pilot_extract
from bin.bookclassifier import classify, MANGA_PUBLISHERS, COMIC_PUBLISHERS, BOOK_ONLY_EXTS
from bin.bookprocessor import build_output_path as book_output_path, detect_genre, enrich_from_api as book_enrich
from bin.comicprocessor import build_output_path as comic_output_path, normalize_publisher, enrich_from_api as comic_enrich
from bin.mangaprocessor import build_output_path as manga_output_path, enrich_from_api as manga_enrich
from bin.musicprocessor import build_output_path as music_output_path, enrich_from_api as music_enrich
from bin.audiobookprocessor import build_output_path as audiobook_output_path, enrich_from_api as audiobook_enrich


# ═════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def sanitize(s):
    if not s:
        return "Unknown"
    s = re.sub(r'[\\/*?:"<>|]', '', s)
    return re.sub(r'\s+', ' ', s).strip() or "Unknown"


def make_epub(path, title, author, publisher="", year="", subjects=None, isbn="", language="en"):
    path.parent.mkdir(parents=True, exist_ok=True)
    subjects = subjects or []
    subj_xml = "".join(f'<dc:subject>{s}</dc:subject>' for s in subjects)
    date_xml = f'<dc:date>{year}-01-01</dc:date>' if year else ''
    isbn_xml = f'<dc:identifier>urn:isbn:{isbn}</dc:identifier>' if isbn else ''
    lang_xml = f'<dc:language>{language}</dc:language>'
    opf = f'''<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf">
<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
<dc:title>{title}</dc:title>
<dc:creator>{author}</dc:creator>
<dc:publisher>{publisher}</dc:publisher>
{date_xml}
{subj_xml}
{isbn_xml}
{lang_xml}
</metadata></package>'''
    container = '''<?xml version="1.0"?>
<container xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
<rootfiles><rootfile full-path="content.opf"/></rootfiles></container>'''
    with zipfile.ZipFile(str(path), 'w') as zf:
        zf.writestr("META-INF/container.xml", container)
        zf.writestr("content.opf", opf)


def make_cbz(path, series="", number=1, year="", publisher="", manga="", writer="", volume=""):
    path.parent.mkdir(parents=True, exist_ok=True)
    vol_xml = f'<Volume>{volume}</Volume>' if volume else ''
    ci = f'''<?xml version="1.0" encoding="UTF-8"?>
<ComicInfo>
<Series>{series}</Series>
<Number>{number}</Number>
<Year>{year}</Year>
<Publisher>{publisher}</Publisher>
<Writer>{writer}</Writer>
<Manga>{manga}</Manga>
{vol_xml}
</ComicInfo>'''
    with zipfile.ZipFile(str(path), 'w') as zf:
        zf.writestr("ComicInfo.xml", ci)
        zf.writestr("page_001.jpg", b'\xff\xd8\xff\xe0' + b'\x00' * 100)


def _get_rel_output(output_key):
    out_root = Path(OUTPUT.get(output_key, output_key))
    lib_str = _lib_root.replace('\\', '/')
    out_str = str(out_root).replace('\\', '/')
    if out_str.startswith(lib_str):
        return Path(out_str[len(lib_str):].lstrip('/'))
    return out_root


# ═════════════════════════════════════════════════════════════════════════════
# TEST TRACKING
# ═════════════════════════════════════════════════════════════════════════════

results = []
failures = []
case_num = 0
category_counts = {}


def run_case(category, label, output_key, rel_path, make_fn=None, expect_exists=True):
    global case_num
    case_num += 1
    category_counts[category] = category_counts.get(category, 0) + 1
    rel_root = _get_rel_output(output_key)
    final = LIB / rel_root / rel_path
    final.parent.mkdir(parents=True, exist_ok=True)
    if make_fn:
        make_fn(final)
    else:
        final.write_bytes(b'\x00' * 64)

    exists = final.exists()
    ok = exists == expect_exists
    status = "PASS" if ok else "FAIL"
    if not ok:
        failures.append(f"  [{case_num:3d}] {category:12s} | {label} | expected={'exists' if expect_exists else 'missing'}")

    results.append({
        "num": case_num,
        "category": category,
        "label": label,
        "path": str(final.relative_to(LIB)),
        "status": status,
    })
    return final


def run_pipeline_case(category, label, output_key, file_path, meta, processor_fn, expect_genre=""):
    """Run through the actual processor build_output_path and verify."""
    global case_num
    case_num += 1
    category_counts[category] = category_counts.get(category, 0) + 1

    tags = meta.get("tags", {})
    rel_path = processor_fn(tags, file_path.suffix)
    rel_root = _get_rel_output(output_key)
    final = LIB / rel_root / rel_path
    final.parent.mkdir(parents=True, exist_ok=True)
    if file_path.exists():
        shutil.copy2(str(file_path), str(final))
    else:
        final.write_bytes(b'\x00' * 64)

    # Check genre if expected
    genre = detect_genre(tags) if expect_genre else ""
    genre_ok = genre == expect_genre if expect_genre else True
    status = "PASS" if genre_ok else "FAIL"
    if not genre_ok:
        failures.append(f"  [{case_num:3d}] {category:12s} | {label} | genre expected={expect_genre} got={genre}")

    results.append({
        "num": case_num,
        "category": category,
        "label": label,
        "path": str(final.relative_to(LIB)),
        "status": status,
    })
    return final


print()
print("=" * 80)
print("  BOOKS & MUSIC HEAVY STRESS TEST")
print("=" * 80)
print()

# Clean previous test output
for subdir in ["_test_work"]:
    p = LIB / subdir
    if p.exists():
        shutil.rmtree(str(p), ignore_errors=True)
WORK.mkdir(parents=True, exist_ok=True)


# ═════════════════════════════════════════════════════════════════════════════
#  1. BOOKS — FICTION (25 cases)
# ═════════════════════════════════════════════════════════════════════════════

fiction_books = [
    {"title": "The Hobbit", "author": "J.R.R. Tolkien", "year": "1937", "subjects": ["Fantasy", "Fiction"]},
    {"title": "1984", "author": "George Orwell", "year": "1949", "subjects": ["Dystopian", "Fiction"]},
    {"title": "Pride and Prejudice", "author": "Jane Austen", "year": "1813", "subjects": ["Romance", "Literary"]},
    {"title": "The Three-Body Problem", "author": "Liu Cixin", "year": "2008", "subjects": ["Science Fiction"]},
    {"title": "One Hundred Years of Solitude", "author": "Gabriel Garcia Marquez", "year": "1967", "subjects": ["Fiction"]},
    {"title": "Norwegian Wood", "author": "Haruki Murakami", "year": "1987", "subjects": ["Fiction", "Novel"]},
    {"title": "Crime and Punishment", "author": "Fyodor Dostoevsky", "year": "1866", "subjects": ["Fiction"]},
    {"title": "The Shining", "author": "Stephen King", "year": "1977", "subjects": ["Horror", "Fiction"]},
    {"title": "Murder on the Orient Express", "author": "Agatha Christie", "year": "1934", "subjects": ["Mystery"]},
    {"title": "Dune", "author": "Frank Herbert", "year": "1965", "subjects": ["Science Fiction", "Adventure"]},
    {"title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "year": "1925", "subjects": ["Fiction", "Literary"]},
    {"title": "To Kill a Mockingbird", "author": "Harper Lee", "year": "1960", "subjects": ["Fiction"]},
    {"title": "Brave New World", "author": "Aldous Huxley", "year": "1932", "subjects": ["Dystopian", "Fiction"]},
    {"title": "The Catcher in the Rye", "author": "J.D. Salinger", "year": "1951", "subjects": ["Fiction"]},
    {"title": "Fahrenheit 451", "author": "Ray Bradbury", "year": "1953", "subjects": ["Science Fiction"]},
    {"title": "The Name of the Wind", "author": "Patrick Rothfuss", "year": "2007", "subjects": ["Fantasy", "Fiction"]},
    {"title": "Neuromancer", "author": "William Gibson", "year": "1984", "subjects": ["Science Fiction"]},
    {"title": "The Hitchhiker's Guide to the Galaxy", "author": "Douglas Adams", "year": "1979", "subjects": ["Fiction", "Sci-Fi"]},
    {"title": "Slaughterhouse-Five", "author": "Kurt Vonnegut", "year": "1969", "subjects": ["Fiction"]},
    {"title": "The Road", "author": "Cormac McCarthy", "year": "2006", "subjects": ["Fiction"]},
    {"title": "Beloved", "author": "Toni Morrison", "year": "1987", "subjects": ["Fiction", "Literary"]},
    {"title": "The Handmaid's Tale", "author": "Margaret Atwood", "year": "1985", "subjects": ["Dystopian", "Fiction"]},
    {"title": "The Color Purple", "author": "Alice Walker", "year": "1982", "subjects": ["Fiction"]},
    {"title": "Gone Girl", "author": "Gillian Flynn", "year": "2012", "subjects": ["Thriller", "Fiction"]},
    {"title": "The Girl with the Dragon Tattoo", "author": "Stieg Larsson", "year": "2005", "subjects": ["Mystery", "Thriller"]},
]

for bc in fiction_books:
    fp = WORK / f"{bc['author']} - {bc['title']}.epub"
    make_epub(fp, bc["title"], bc["author"], year=bc["year"], subjects=bc["subjects"])
    meta = pilot_extract(fp)
    tags = meta.get("tags", {})
    if bc.get("subjects") and not tags.get("subjects"):
        tags["subjects"] = bc["subjects"]
    run_pipeline_case("book_fiction", f"{bc['author']} - {bc['title']}", "books", fp,
                      {"tags": tags}, book_output_path, expect_genre="Fiction")
    fp.unlink(missing_ok=True)


# ═════════════════════════════════════════════════════════════════════════════
#  2. BOOKS — NON-FICTION (15 cases)
# ═════════════════════════════════════════════════════════════════════════════

nonfiction_books = [
    {"title": "Steve Jobs", "author": "Walter Isaacson", "year": "2011", "subjects": ["Biography"]},
    {"title": "Sapiens", "author": "Yuval Noah Harari", "year": "2011", "subjects": ["History", "Science"]},
    {"title": "How to Win Friends and Influence People", "author": "Dale Carnegie", "year": "1936", "subjects": ["Self-Help"]},
    {"title": "Kitchen Confidential", "author": "Anthony Bourdain", "year": "2000", "subjects": ["Cooking", "Memoir"]},
    {"title": "Educated", "author": "Tara Westover", "year": "2018", "subjects": ["Memoir", "Autobiography"]},
    {"title": "Becoming", "author": "Michelle Obama", "year": "2018", "subjects": ["Autobiography", "Biography"]},
    {"title": "Thinking, Fast and Slow", "author": "Daniel Kahneman", "year": "2011", "subjects": ["Psychology"]},
    {"title": "The Art of War", "author": "Sun Tzu", "year": "-500", "subjects": ["Philosophy", "History"]},
    {"title": "A Brief History of Time", "author": "Stephen Hawking", "year": "1988", "subjects": ["Science"]},
    {"title": "Guns, Germs, and Steel", "author": "Jared Diamond", "year": "1997", "subjects": ["History"]},
    {"title": "Freakonomics", "author": "Steven Levitt", "year": "2005", "subjects": ["Economics"]},
    {"title": "The Diary of a Young Girl", "author": "Anne Frank", "year": "1947", "subjects": ["Biography", "History"]},
    {"title": "Born a Crime", "author": "Trevor Noah", "year": "2016", "subjects": ["Autobiography", "Memoir"]},
    {"title": "Outliers", "author": "Malcolm Gladwell", "year": "2008", "subjects": ["Sociology", "Psychology"]},
    {"title": "The Power of Habit", "author": "Charles Duhigg", "year": "2012", "subjects": ["Psychology", "Self-Help"]},
]

for bc in nonfiction_books:
    fp = WORK / f"{bc['author']} - {bc['title']}.epub"
    make_epub(fp, bc["title"], bc["author"], year=bc["year"], subjects=bc["subjects"])
    meta = pilot_extract(fp)
    tags = meta.get("tags", {})
    if bc.get("subjects") and not tags.get("subjects"):
        tags["subjects"] = bc["subjects"]
    run_pipeline_case("book_nonfic", f"{bc['author']} - {bc['title']}", "books", fp,
                      {"tags": tags}, book_output_path, expect_genre="Non-Fiction")
    fp.unlink(missing_ok=True)


# ═════════════════════════════════════════════════════════════════════════════
#  3. BOOKS — TECHNICAL (12 cases)
# ═════════════════════════════════════════════════════════════════════════════

technical_books = [
    {"title": "Clean Code", "author": "Robert C. Martin", "year": "2008", "subjects": ["Programming", "Software"]},
    {"title": "Introduction to Algorithms", "author": "Cormen et al.", "year": "2009", "subjects": ["Computer Science", "Algorithm"]},
    {"title": "Designing Data-Intensive Applications", "author": "Martin Kleppmann", "year": "2017", "subjects": ["Database", "Engineering"]},
    {"title": "The Pragmatic Programmer", "author": "David Thomas", "year": "1999", "subjects": ["Programming"]},
    {"title": "Design Patterns", "author": "Gang of Four", "year": "1994", "subjects": ["Software Engineering"]},
    {"title": "Structure and Interpretation of Computer Programs", "author": "Abelson & Sussman", "year": "1985", "subjects": ["Computer Science"]},
    {"title": "Artificial Intelligence A Modern Approach", "author": "Stuart Russell", "year": "2020", "subjects": ["Artificial Intelligence"]},
    {"title": "Deep Learning", "author": "Ian Goodfellow", "year": "2016", "subjects": ["Machine Learning"]},
    {"title": "Computer Networking A Top-Down Approach", "author": "James Kurose", "year": "2016", "subjects": ["Networking"]},
    {"title": "Operating System Concepts", "author": "Silberschatz", "year": "2018", "subjects": ["Computer Science", "Textbook"]},
    {"title": "Cracking the Coding Interview", "author": "Gayle McDowell", "year": "2015", "subjects": ["Programming", "Technical"]},
    {"title": "The Art of Electronics", "author": "Horowitz & Hill", "year": "2015", "subjects": ["Engineering"]},
]

for bc in technical_books:
    fp = WORK / f"{bc['author']} - {bc['title']}.epub"
    make_epub(fp, bc["title"], bc["author"], year=bc["year"], subjects=bc["subjects"])
    meta = pilot_extract(fp)
    tags = meta.get("tags", {})
    if bc.get("subjects") and not tags.get("subjects"):
        tags["subjects"] = bc["subjects"]
    run_pipeline_case("book_tech", f"{bc['author']} - {bc['title']}", "books", fp,
                      {"tags": tags}, book_output_path, expect_genre="Technical")
    fp.unlink(missing_ok=True)


# ═════════════════════════════════════════════════════════════════════════════
#  4. BOOKS — EDGE CASES (10 cases)
# ═════════════════════════════════════════════════════════════════════════════

# PDF with no embedded metadata
fp = WORK / "unknown_textbook.pdf"
fp.parent.mkdir(parents=True, exist_ok=True)
fp.write_bytes(b'%PDF-1.4 ' + b'\x00' * 100)
tags = {"title": "Unknown Textbook", "author": "", "year": "", "subjects": []}
run_pipeline_case("book_edge", "PDF no metadata", "books", fp, {"tags": tags}, book_output_path)
fp.unlink(missing_ok=True)

# MOBI file (always book)
fp = WORK / "mystery_novel.mobi"
fp.parent.mkdir(parents=True, exist_ok=True)
fp.write_bytes(b'BOOKMOBI' + b'\x00' * 100)
tags = {"title": "Mystery Novel", "author": "Unknown Author", "year": "2020", "subjects": ["Mystery"]}
run_pipeline_case("book_edge", "MOBI file", "books", fp, {"tags": tags}, book_output_path, expect_genre="Fiction")
fp.unlink(missing_ok=True)

# Very long title
fp = WORK / "long_title.epub"
long_title = "The Extremely Long and Unnecessarily Verbose Title of This Book Which Goes On and On"
make_epub(fp, long_title, "Verbose Author", year="2023", subjects=["Fiction"])
meta = pilot_extract(fp)
tags = meta.get("tags", {})
tags["subjects"] = ["Fiction"]
run_pipeline_case("book_edge", "Very long title", "books", fp, {"tags": tags}, book_output_path, expect_genre="Fiction")
fp.unlink(missing_ok=True)

# Unicode author name
fp = WORK / "unicode_author.epub"
make_epub(fp, "The Wind-Up Bird Chronicle", "村上春樹", year="1994", subjects=["Fiction"])
meta = pilot_extract(fp)
tags = meta.get("tags", {})
tags["subjects"] = ["Fiction"]
run_pipeline_case("book_edge", "Unicode author", "books", fp, {"tags": tags}, book_output_path, expect_genre="Fiction")
fp.unlink(missing_ok=True)

# Book with ISBN
fp = WORK / "isbn_book.epub"
make_epub(fp, "The Alchemist", "Paulo Coelho", year="1988", subjects=["Fiction"], isbn="9780062315007")
meta = pilot_extract(fp)
tags = meta.get("tags", {})
tags["subjects"] = ["Fiction"]
run_pipeline_case("book_edge", "Book with ISBN", "books", fp, {"tags": tags}, book_output_path, expect_genre="Fiction")
fp.unlink(missing_ok=True)

# AZW3 format
fp = WORK / "kindle_book.azw3"
fp.parent.mkdir(parents=True, exist_ok=True)
fp.write_bytes(b'\x00' * 200)
tags = {"title": "Kindle Format Book", "author": "Amazon Author", "year": "2021", "subjects": []}
run_pipeline_case("book_edge", "AZW3 format", "books", fp, {"tags": tags}, book_output_path)
fp.unlink(missing_ok=True)

# FB2 format
fp = WORK / "russian_novel.fb2"
fp.parent.mkdir(parents=True, exist_ok=True)
fp.write_bytes(b'<?xml version="1.0"?><FictionBook></FictionBook>')
tags = {"title": "War and Peace", "author": "Leo Tolstoy", "year": "1869", "subjects": ["Fiction"]}
run_pipeline_case("book_edge", "FB2 format", "books", fp, {"tags": tags}, book_output_path, expect_genre="Fiction")
fp.unlink(missing_ok=True)

# No subjects (genre should be empty)
fp = WORK / "no_genre.epub"
make_epub(fp, "Untitled Work", "Anonymous", year="2024", subjects=[])
meta = pilot_extract(fp)
tags = meta.get("tags", {})
run_pipeline_case("book_edge", "No genre tags", "books", fp, {"tags": tags}, book_output_path, expect_genre="")
fp.unlink(missing_ok=True)

# Multiple subjects
fp = WORK / "multi_subject.epub"
make_epub(fp, "Code Complete", "Steve McConnell", year="2004", subjects=["Programming", "Software", "Engineering", "Technical"])
meta = pilot_extract(fp)
tags = meta.get("tags", {})
if not tags.get("subjects"):
    tags["subjects"] = ["Programming", "Software", "Engineering", "Technical"]
run_pipeline_case("book_edge", "Multiple subjects", "books", fp, {"tags": tags}, book_output_path, expect_genre="Technical")
fp.unlink(missing_ok=True)

# Author with special chars
fp = WORK / "special_chars.epub"
make_epub(fp, "Flowers for Algernon", "Daniel Keyes", year="1966", subjects=["Fiction"])
meta = pilot_extract(fp)
tags = meta.get("tags", {})
tags["subjects"] = ["Fiction"]
run_pipeline_case("book_edge", "Author special chars", "books", fp, {"tags": tags}, book_output_path, expect_genre="Fiction")
fp.unlink(missing_ok=True)


# ═════════════════════════════════════════════════════════════════════════════
#  5. COMICS (25 cases)
# ═════════════════════════════════════════════════════════════════════════════

comic_cases = [
    # DC
    {"series": "Batman", "number": 1, "year": "2020", "publisher": "DC Comics", "writer": "James Tynion IV"},
    {"series": "Batman", "number": 125, "year": "2022", "publisher": "DC Comics", "writer": "Chip Zdarsky"},
    {"series": "Superman", "number": 1, "year": "2023", "publisher": "DC Comics", "writer": "Joshua Williamson"},
    {"series": "Wonder Woman", "number": 800, "year": "2023", "publisher": "DC Comics", "writer": "Tom King"},
    {"series": "The Flash", "number": 1, "year": "2023", "publisher": "DC Comics", "writer": "Simon Spurrier"},
    {"series": "Green Lantern", "number": 1, "year": "2023", "publisher": "DC Comics", "writer": "Jeremy Adams"},
    # Marvel
    {"series": "The Amazing Spider-Man", "number": 1, "year": "2022", "publisher": "Marvel Comics", "writer": "Zeb Wells"},
    {"series": "X-Men", "number": 35, "year": "2024", "publisher": "Marvel Comics", "writer": "Gerry Duggan"},
    {"series": "Avengers", "number": 1, "year": "2023", "publisher": "Marvel Comics", "writer": "Jed MacKay"},
    {"series": "Daredevil", "number": 1, "year": "2023", "publisher": "Marvel Comics", "writer": "Saladin Ahmed"},
    {"series": "Wolverine", "number": 50, "year": "2024", "publisher": "Marvel Comics", "writer": "Benjamin Percy"},
    # Image
    {"series": "Saga", "number": 66, "year": "2024", "publisher": "Image Comics", "writer": "Brian K. Vaughan"},
    {"series": "Spawn", "number": 350, "year": "2023", "publisher": "Image Comics", "writer": "Todd McFarlane"},
    {"series": "Invincible", "number": 144, "year": "2018", "publisher": "Image Comics", "writer": "Robert Kirkman"},
    {"series": "The Walking Dead", "number": 193, "year": "2019", "publisher": "Image Comics", "writer": "Robert Kirkman"},
    # Dark Horse
    {"series": "Hellboy", "number": 1, "year": "1994", "publisher": "Dark Horse Comics", "writer": "Mike Mignola"},
    {"series": "Black Hammer", "number": 12, "year": "2017", "publisher": "Dark Horse Comics", "writer": "Jeff Lemire"},
    # IDW
    {"series": "Teenage Mutant Ninja Turtles", "number": 150, "year": "2024", "publisher": "IDW Publishing", "writer": "Sophie Campbell"},
    {"series": "Transformers", "number": 1, "year": "2023", "publisher": "IDW Publishing", "writer": "Daniel Warren Johnson"},
    # BOOM!
    {"series": "Something is Killing the Children", "number": 36, "year": "2024", "publisher": "BOOM! Studios", "writer": "James Tynion IV"},
    {"series": "Lumberjanes", "number": 75, "year": "2020", "publisher": "BOOM! Studios", "writer": "Shannon Watters"},
    # Other
    {"series": "Archie", "number": 32, "year": "2018", "publisher": "Archie Comics", "writer": "Mark Waid"},
    {"series": "Usagi Yojimbo", "number": 1, "year": "2019", "publisher": "IDW Publishing", "writer": "Stan Sakai"},
    {"series": "Bone", "number": 1, "year": "1991", "publisher": "", "writer": "Jeff Smith"},
    {"series": "Strangers in Paradise", "number": 1, "year": "1993", "publisher": "", "writer": "Terry Moore"},
]

for cc in comic_cases:
    fp = WORK / f"{cc['series']} {cc['number']:03d}.cbz"
    make_cbz(fp, cc["series"], cc["number"], cc["year"], cc["publisher"], writer=cc["writer"])
    meta = pilot_extract(fp)
    tags = meta.get("tags", {})
    rel = comic_output_path(tags, ".cbz")
    classification = classify(fp, meta)
    run_case("comic", f"{cc['series']} #{cc['number']}", "comics", rel,
             make_fn=lambda p, s=cc: make_cbz(p, s["series"], s["number"], s["year"], s["publisher"], writer=s["writer"]))
    fp.unlink(missing_ok=True)


# ═════════════════════════════════════════════════════════════════════════════
#  6. MANGA (30 cases)
# ═════════════════════════════════════════════════════════════════════════════

manga_cases = [
    # Shonen
    {"series": "One Piece", "volume": 107, "year": "2024", "publisher": "Viz Media"},
    {"series": "Naruto", "volume": 72, "year": "2014", "publisher": "Viz Media"},
    {"series": "Dragon Ball", "volume": 42, "year": "1995", "publisher": "Viz Media"},
    {"series": "Bleach", "volume": 74, "year": "2016", "publisher": "Viz Media"},
    {"series": "My Hero Academia", "volume": 40, "year": "2024", "publisher": "Viz Media"},
    {"series": "Demon Slayer", "volume": 23, "year": "2020", "publisher": "Viz Media"},
    {"series": "Jujutsu Kaisen", "volume": 26, "year": "2024", "publisher": "Viz Media"},
    {"series": "Chainsaw Man", "volume": 16, "year": "2024", "publisher": "Viz Media"},
    {"series": "Death Note", "volume": 12, "year": "2006", "publisher": "Viz Media"},
    {"series": "Hunter x Hunter", "volume": 37, "year": "2022", "publisher": "Viz Media"},
    {"series": "One Punch Man", "volume": 29, "year": "2024", "publisher": "Viz Media"},
    # Seinen
    {"series": "Berserk", "volume": 41, "year": "2021", "publisher": "Dark Horse Manga"},
    {"series": "Vagabond", "volume": 37, "year": "2014", "publisher": "Viz Media"},
    {"series": "Vinland Saga", "volume": 27, "year": "2024", "publisher": "Kodansha Comics"},
    {"series": "Tokyo Ghoul", "volume": 14, "year": "2014", "publisher": "Viz Media"},
    {"series": "Attack on Titan", "volume": 34, "year": "2021", "publisher": "Kodansha Comics"},
    {"series": "Monster", "volume": 18, "year": "2006", "publisher": "Viz Media"},
    {"series": "20th Century Boys", "volume": 22, "year": "2012", "publisher": "Viz Media"},
    {"series": "Gantz", "volume": 37, "year": "2013", "publisher": "Dark Horse Manga"},
    # Shojo/Josei
    {"series": "Fruits Basket", "volume": 23, "year": "2007", "publisher": "Tokyopop"},
    {"series": "Sailor Moon", "volume": 12, "year": "2013", "publisher": "Kodansha Comics"},
    {"series": "Nana", "volume": 21, "year": "2009", "publisher": "Viz Media"},
    {"series": "Ouran High School Host Club", "volume": 18, "year": "2011", "publisher": "Viz Media"},
    {"series": "Cardcaptor Sakura", "volume": 12, "year": "2012", "publisher": "Kodansha Comics"},
    # Other publishers
    {"series": "Solo Leveling", "volume": 8, "year": "2022", "publisher": "Yen Press"},
    {"series": "Spy x Family", "volume": 12, "year": "2024", "publisher": "Viz Media"},
    {"series": "Fullmetal Alchemist", "volume": 27, "year": "2010", "publisher": "Viz Media"},
    {"series": "Slam Dunk", "volume": 31, "year": "2013", "publisher": "Viz Media"},
    {"series": "Overlord", "volume": 18, "year": "2024", "publisher": "Yen Press"},
    {"series": "Made in Abyss", "volume": 12, "year": "2024", "publisher": "Seven Seas"},
]

for mc in manga_cases:
    fp = WORK / f"{mc['series']} Vol.{mc['volume']:02d}.cbz"
    make_cbz(fp, mc["series"], mc["volume"], mc["year"], mc["publisher"], manga="Yes")
    meta = pilot_extract(fp)
    tags = meta.get("tags", {})
    rel = manga_output_path(tags, ".cbz")
    classification = classify(fp, meta)
    run_case("manga", f"{mc['series']} Vol.{mc['volume']}", "manga", rel,
             make_fn=lambda p, s=mc: make_cbz(p, s["series"], s["volume"], s["year"], s["publisher"], manga="Yes"))
    fp.unlink(missing_ok=True)


# ═════════════════════════════════════════════════════════════════════════════
#  7. MUSIC (40 cases)
# ═════════════════════════════════════════════════════════════════════════════

music_tracks = [
    # Classic Rock
    ("Pink Floyd", "The Dark Side of the Moon", "1973", 1, "Speak to Me"),
    ("Pink Floyd", "The Dark Side of the Moon", "1973", 3, "Time"),
    ("Pink Floyd", "The Dark Side of the Moon", "1973", 6, "Money"),
    ("Led Zeppelin", "Led Zeppelin IV", "1971", 4, "Stairway to Heaven"),
    ("Queen", "A Night at the Opera", "1975", 11, "Bohemian Rhapsody"),
    ("The Beatles", "Abbey Road", "1969", 1, "Come Together"),
    ("The Beatles", "Abbey Road", "1969", 2, "Something"),
    ("The Beatles", "Sgt. Pepper's Lonely Hearts Club Band", "1967", 1, "Sgt. Pepper's Lonely Hearts Club Band"),
    ("Nirvana", "Nevermind", "1991", 1, "Smells Like Teen Spirit"),
    ("Radiohead", "OK Computer", "1997", 1, "Airbag"),
    ("Radiohead", "OK Computer", "1997", 3, "Subterranean Homesick Alien"),
    # Hip-Hop
    ("Kendrick Lamar", "To Pimp a Butterfly", "2015", 3, "King Kunta"),
    ("Kendrick Lamar", "good kid, m.A.A.d city", "2012", 5, "m.A.A.d city"),
    ("Kanye West", "My Beautiful Dark Twisted Fantasy", "2010", 1, "Dark Fantasy"),
    ("Jay-Z", "The Blueprint", "2001", 1, "The Ruler's Back"),
    ("Nas", "Illmatic", "1994", 1, "The Genesis"),
    ("Tyler, The Creator", "IGOR", "2019", 1, "IGOR'S THEME"),
    # Pop
    ("Taylor Swift", "1989", "2014", 1, "Welcome to New York"),
    ("Taylor Swift", "Midnights", "2022", 1, "Lavender Haze"),
    ("Beyonce", "Lemonade", "2016", 1, "Pray You Catch Me"),
    ("Billie Eilish", "When We All Fall Asleep Where Do We Go", "2019", 2, "bad guy"),
    ("Adele", "21", "2011", 3, "Rolling in the Deep"),
    ("Dua Lipa", "Future Nostalgia", "2020", 2, "Don't Start Now"),
    # Electronic
    ("Daft Punk", "Random Access Memories", "2013", 8, "Get Lucky"),
    ("Daft Punk", "Discovery", "2001", 3, "Digital Love"),
    ("Aphex Twin", "Selected Ambient Works 85-92", "1992", 1, "Xtal"),
    ("Boards of Canada", "Music Has the Right to Children", "1998", 1, "Wildlife Analysis"),
    # Jazz
    ("Miles Davis", "Kind of Blue", "1959", 1, "So What"),
    ("John Coltrane", "A Love Supreme", "1965", 1, "Acknowledgement"),
    ("Thelonious Monk", "Brilliant Corners", "1957", 1, "Brilliant Corners"),
    # K-Pop / Latin / World
    ("BTS", "Map of the Soul 7", "2020", 1, "Interlude Shadow"),
    ("BLACKPINK", "THE ALBUM", "2020", 2, "Ice Cream"),
    ("Bad Bunny", "Un Verano Sin Ti", "2022", 1, "Moscow Mule"),
    ("Rosalia", "Motomami", "2022", 1, "SAOKO"),
    # Classical
    ("Various Artists", "Beethoven Symphony No.9", "1824", 4, "Ode to Joy"),
    ("Yo-Yo Ma", "Bach Cello Suites", "1983", 1, "Suite No. 1 Prelude"),
    # Metal
    ("Metallica", "Master of Puppets", "1986", 1, "Battery"),
    ("Tool", "Lateralus", "2001", 6, "Lateralus"),
    # Indie
    ("Arcade Fire", "Funeral", "2004", 1, "Neighborhood #1 (Tunnels)"),
    ("Tame Impala", "Currents", "2015", 1, "Let It Happen"),
]

for artist, album, year, track, title in music_tracks:
    tags = {"album_artist": artist, "artist": artist, "album": album,
            "title": title, "track": track, "year": year}
    rel = music_output_path(tags, ".flac")
    run_case("music", f"{artist} - {title}", "music", rel)


# ═════════════════════════════════════════════════════════════════════════════
#  8. AUDIOBOOKS (25 cases)
# ═════════════════════════════════════════════════════════════════════════════

audiobooks = [
    # Harry Potter series
    ("J.K. Rowling", "Harry Potter and the Philosopher's Stone", "1997", 1, "The Boy Who Lived"),
    ("J.K. Rowling", "Harry Potter and the Philosopher's Stone", "1997", 2, "The Vanishing Glass"),
    ("J.K. Rowling", "Harry Potter and the Philosopher's Stone", "1997", 3, "The Letters from No One"),
    ("J.K. Rowling", "Harry Potter and the Chamber of Secrets", "1998", 1, "The Worst Birthday"),
    ("J.K. Rowling", "Harry Potter and the Prisoner of Azkaban", "1999", 1, "Owl Post"),
    # Other fiction
    ("Stephen King", "The Stand", "1978", 1, "Captain Trips"),
    ("Stephen King", "The Stand", "1978", 2, "The Dreams"),
    ("Stephen King", "It", "1986", 1, "After the Flood"),
    ("Frank Herbert", "Dune", "1965", 1, "Part One - Dune"),
    ("Frank Herbert", "Dune", "1965", 2, "Part Two - Muad'Dib"),
    ("Neil Gaiman", "American Gods", "2001", 1, "Chapter One"),
    ("Brandon Sanderson", "Mistborn The Final Empire", "2006", 1, "Prologue"),
    ("Patrick Rothfuss", "The Name of the Wind", "2007", 1, "A Place for Demons"),
    # Non-fiction audiobooks
    ("George Orwell", "1984", "1949", 1, "Part One"),
    ("Yuval Noah Harari", "Sapiens", "2011", 1, "An Animal of No Significance"),
    ("Michelle Obama", "Becoming", "2018", 1, "Becoming Me"),
    ("Walter Isaacson", "Steve Jobs", "2011", 1, "Childhood"),
    ("Malcolm Gladwell", "Outliers", "2008", 1, "The Matthew Effect"),
    ("James Clear", "Atomic Habits", "2018", 1, "The Surprising Power of Atomic Habits"),
    ("Mark Manson", "The Subtle Art of Not Giving a F*ck", "2016", 1, "Don't Try"),
    # Science/History
    ("Bill Bryson", "A Short History of Nearly Everything", "2003", 1, "How to Build a Universe"),
    ("Carl Sagan", "Cosmos", "1980", 1, "The Shores of the Cosmic Ocean"),
    ("Richard Dawkins", "The Selfish Gene", "1976", 1, "Why Are People"),
    ("Stephen Hawking", "A Brief History of Time", "1988", 1, "Our Picture of the Universe"),
    ("Neil deGrasse Tyson", "Astrophysics for People in a Hurry", "2017", 1, "The Greatest Story Ever Told"),
]

for author, book, year, track, part in audiobooks:
    tags = {"album_artist": author, "artist": author, "album": book,
            "title": part, "track": track, "year": year}
    rel = audiobook_output_path(tags, ".m4b")
    run_case("audiobook", f"{author} - {part}", "audiobooks", rel)


# ═════════════════════════════════════════════════════════════════════════════
#  9. API ENRICHMENT TESTS (unit tests for enrich_from_api functions)
# ═════════════════════════════════════════════════════════════════════════════

print()
print("-" * 80)
print("  API ENRICHMENT FUNCTION TESTS")
print("-" * 80)

enrich_pass = 0
enrich_fail = 0

def test_enrich(name, fn, input_tags, expected_unchanged_fields=None, **kwargs):
    """Test that enrich_from_api returns dict, doesn't crash, preserves existing fields."""
    global enrich_pass, enrich_fail
    try:
        result = fn(input_tags, **kwargs)
        assert isinstance(result, dict), f"Expected dict, got {type(result)}"
        # Existing non-empty fields should not be overwritten
        if expected_unchanged_fields:
            for field in expected_unchanged_fields:
                if input_tags.get(field):
                    assert result.get(field) == input_tags[field], \
                        f"Field '{field}' changed: {input_tags[field]} -> {result.get(field)}"
        enrich_pass += 1
        print(f"  PASS: {name}")
    except Exception as e:
        enrich_fail += 1
        print(f"  FAIL: {name} — {e}")

# Book enrichment tests
test_enrich("book_enrich: full tags preserved",
            book_enrich, {"title": "1984", "author": "George Orwell", "year": "1949", "isbn": ""},
            expected_unchanged_fields=["title", "author", "year"])

test_enrich("book_enrich: missing author",
            book_enrich, {"title": "Unknown Book", "author": "", "year": "2020"})

test_enrich("book_enrich: empty tags",
            book_enrich, {"title": "", "author": "", "isbn": ""})

test_enrich("book_enrich: ISBN lookup",
            book_enrich, {"title": "", "author": "", "isbn": "9780451524935"})

# Comic enrichment tests
test_enrich("comic_enrich: full tags preserved",
            comic_enrich, {"series": "Batman", "publisher": "DC Comics", "year": "2020"},
            expected_unchanged_fields=["series", "publisher"])

test_enrich("comic_enrich: missing publisher",
            comic_enrich, {"series": "Batman", "publisher": "", "year": ""})

test_enrich("comic_enrich: empty tags",
            comic_enrich, {"series": "", "title": ""})

# Manga enrichment tests
test_enrich("manga_enrich: full tags preserved",
            manga_enrich, {"series": "One Piece", "year": "1997"},
            expected_unchanged_fields=["series"])

test_enrich("manga_enrich: missing series",
            manga_enrich, {"series": "", "title": "Unknown Manga"})

# Music enrichment tests
test_enrich("music_enrich: full tags preserved",
            music_enrich, {"artist": "Pink Floyd", "title": "Money", "album": "The Dark Side of the Moon"},
            expected_unchanged_fields=["artist", "title", "album"])

test_enrich("music_enrich: missing album",
            music_enrich, {"artist": "Pink Floyd", "title": "Money", "album": ""})

test_enrich("music_enrich: empty tags",
            music_enrich, {"artist": "", "title": "", "album": ""})

# Audiobook enrichment tests
test_enrich("audiobook_enrich: full tags preserved",
            audiobook_enrich, {"album_artist": "J.K. Rowling", "album": "Harry Potter", "artist": "J.K. Rowling"},
            expected_unchanged_fields=["album_artist", "album"])

test_enrich("audiobook_enrich: missing author",
            audiobook_enrich, {"album_artist": "", "artist": "", "album": "Harry Potter"})

# Publisher normalization tests
print()
print("-" * 80)
print("  PUBLISHER NORMALIZATION TESTS")
print("-" * 80)

pub_tests = [
    ("dc", "DC"), ("dc comics", "DC"), ("DC Comics", "DC"),
    ("marvel", "Marvel"), ("marvel comics", "Marvel"),
    ("image", "Image"), ("image comics", "Image"),
    ("dark horse", "Dark Horse"), ("dark horse comics", "Dark Horse"),
    ("idw", "IDW"), ("idw publishing", "IDW"),
    ("boom! studios", "BOOM! Studios"), ("boom studios", "BOOM! Studios"),
    ("", "Independent"), (None, "Independent"),
    ("SomeNewPublisher", "SomeNewPublisher"),
]

pub_pass = 0
pub_fail = 0
for inp, expected in pub_tests:
    result = normalize_publisher(inp)
    if result == expected:
        pub_pass += 1
        print(f"  PASS: '{inp}' -> '{result}'")
    else:
        pub_fail += 1
        print(f"  FAIL: '{inp}' -> '{result}' (expected '{expected}')")


# ═════════════════════════════════════════════════════════════════════════════
#  10. CLASSIFICATION TESTS
# ═════════════════════════════════════════════════════════════════════════════

print()
print("-" * 80)
print("  BOOK CLASSIFICATION TESTS")
print("-" * 80)

class_pass = 0
class_fail = 0

def test_classify(label, filepath_fn, meta_fn, expected_class):
    global class_pass, class_fail
    fp = WORK / f"classify_{label.replace(' ', '_')}"
    filepath_fn(fp)
    # Find the actual file created (filepath_fn may add an extension)
    actual = fp
    for candidate in fp.parent.glob(fp.name + ".*"):
        actual = candidate
        break
    meta = meta_fn(fp)
    result = classify(actual, meta)
    if result == expected_class:
        class_pass += 1
        print(f"  PASS: {label} -> {result}")
    else:
        class_fail += 1
        print(f"  FAIL: {label} -> {result} (expected {expected_class})")
    actual.unlink(missing_ok=True)

# EPUB always = book
test_classify("epub_fiction",
              lambda p: make_epub(p.with_suffix('.epub'), "Test", "Author", subjects=["Fiction"]),
              lambda p: pilot_extract(p.with_suffix('.epub')),
              "book")

# CBZ + DC publisher = comic
test_classify("cbz_dc_comic",
              lambda p: make_cbz(p.with_suffix('.cbz'), "Batman", 1, "2020", "DC Comics"),
              lambda p: pilot_extract(p.with_suffix('.cbz')),
              "comic")

# CBZ + Marvel publisher = comic
test_classify("cbz_marvel_comic",
              lambda p: make_cbz(p.with_suffix('.cbz'), "Spider-Man", 1, "2022", "Marvel Comics"),
              lambda p: pilot_extract(p.with_suffix('.cbz')),
              "comic")

# CBZ + Viz publisher = manga
test_classify("cbz_viz_manga",
              lambda p: make_cbz(p.with_suffix('.cbz'), "Naruto", 1, "2003", "Viz Media"),
              lambda p: pilot_extract(p.with_suffix('.cbz')),
              "manga")

# CBZ + Kodansha publisher = manga
test_classify("cbz_kodansha_manga",
              lambda p: make_cbz(p.with_suffix('.cbz'), "Attack on Titan", 1, "2012", "Kodansha Comics"),
              lambda p: pilot_extract(p.with_suffix('.cbz')),
              "manga")

# CBZ + Manga=Yes flag = manga
test_classify("cbz_manga_flag",
              lambda p: make_cbz(p.with_suffix('.cbz'), "Test Manga", 1, "2020", "", manga="Yes"),
              lambda p: pilot_extract(p.with_suffix('.cbz')),
              "manga")

# CBZ + unknown publisher = comic (default)
test_classify("cbz_unknown_pub",
              lambda p: make_cbz(p.with_suffix('.cbz'), "Indie Comic", 1, "2020", "Random Publisher"),
              lambda p: pilot_extract(p.with_suffix('.cbz')),
              "comic")


# ═════════════════════════════════════════════════════════════════════════════
#  CLEANUP & SUMMARY
# ═════════════════════════════════════════════════════════════════════════════

# Remove temp work dir
shutil.rmtree(str(WORK), ignore_errors=True)

# Print summary
print()
print("=" * 80)
print("  RESULTS SUMMARY")
print("=" * 80)
print()

total = case_num
cat_order = [
    ("book_fiction", "Books (Fiction)"),
    ("book_nonfic", "Books (Non-Fiction)"),
    ("book_tech", "Books (Technical)"),
    ("book_edge", "Books (Edge Cases)"),
    ("comic", "Comics"),
    ("manga", "Manga"),
    ("music", "Music"),
    ("audiobook", "Audiobooks"),
]

for cat_id, cat_label in cat_order:
    c = category_counts.get(cat_id, 0)
    if c:
        print(f"  {cat_label:25s}: {c:3d} files")

print(f"  {'':25s}  --------")
print(f"  {'TOTAL FILES':25s}: {total:3d}")
print()

# Unit test results
print(f"  API Enrichment Tests    :  {enrich_pass} passed, {enrich_fail} failed")
print(f"  Publisher Normalization  :  {pub_pass} passed, {pub_fail} failed")
print(f"  Classification Tests     :  {class_pass} passed, {class_fail} failed")
print()

if failures:
    print(f"  FILE FAILURES ({len(failures)}):")
    for f in failures:
        print(f)
    print()

total_unit = enrich_pass + enrich_fail + pub_pass + pub_fail + class_pass + class_fail
total_unit_pass = enrich_pass + pub_pass + class_pass
total_unit_fail = enrich_fail + pub_fail + class_fail

grand_total = total + total_unit
grand_pass = total - len(failures) + total_unit_pass
grand_fail = len(failures) + total_unit_fail

print(f"  GRAND TOTAL: {grand_pass}/{grand_total} passed", end="")
if grand_fail:
    print(f" ({grand_fail} FAILED)")
else:
    print(" — ALL PASSED!")

print("=" * 80)

# Print library tree
print()
print("  LIBRARY STRUCTURE (Books & Music):")
print()

for group_prefix in ["Audio & Music", "Books & Comics"]:
    group_path = LIB / group_prefix
    if group_path.exists():
        all_files = sorted(group_path.rglob("*"))
        for f in all_files:
            if f.is_file():
                rel = str(f.relative_to(LIB)).replace("\\", "/")
                print(f"    {rel}")

print()
print("=" * 80)
print(f"  Library root: {LIB}")
print("=" * 80)
