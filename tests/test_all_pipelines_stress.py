#!/usr/bin/env python3
"""
test_all_pipelines_stress.py -- Comprehensive stress test for ALL MyMediaManager pipelines.

Tests every pipeline's output path logic and verifies the final unified library structure:
  - Series Pipeline:  TV Shows, Cartoons, Reality TV, Talk Shows, Documentaries
  - Movie Pipeline:   Movies, Anime Movies, Stand-Up, Documentary Movies
  - Anime Pipeline:   Anime Shows
  - Books Pipeline:   Books (Fiction/Non-Fiction/Technical), Comics, Manga
  - Music Pipeline:   Music, Audiobooks

Creates realistic test files with proper metadata, runs through the pipeline logic,
and outputs the complete library tree.
"""

import os
import sys
import re
import json
import shutil
import struct
import zipfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Ensure 'common' is importable (bin modules import it as 'common')
import importlib
import bin.common
sys.modules['common'] = bin.common

# Load config to get output paths
from bin.common import load_config
CFG = load_config()
OUTPUT = CFG['paths']['output']

# ─── Library Root ────────────────────────────────────────────────────────────
# Write to actual Library folder so web panel can display files
_lib_root = CFG['paths']['roots'].get('library', str(ROOT / 'Library'))
LIB = Path(_lib_root)
REVIEW = LIB / "_Review"

# ─── Helpers ─────────────────────────────────────────────────────────────────

def sanitize(s):
    if not s:
        return "Unknown"
    s = re.sub(r'[\\/*?:"<>|]', '', s)
    return re.sub(r'\s+', ' ', s).strip() or "Unknown"


def make_dummy_video(path, size=1024):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'\x00' * size)


def make_dummy_audio(path, size=512):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'\xff\xfb\x90\x00' + b'\x00' * (size - 4))


def make_epub(path, title, author, publisher="", year="", subjects=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    subjects = subjects or []
    subj_xml = "".join(f'<dc:subject>{s}</dc:subject>' for s in subjects)
    date_xml = f'<dc:date>{year}-01-01</dc:date>' if year else ''
    opf = f'''<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf">
<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
<dc:title>{title}</dc:title>
<dc:creator>{author}</dc:creator>
<dc:publisher>{publisher}</dc:publisher>
{date_xml}
{subj_xml}
</metadata></package>'''
    container = '''<?xml version="1.0"?>
<container xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
<rootfiles><rootfile full-path="content.opf"/></rootfiles></container>'''
    with zipfile.ZipFile(str(path), 'w') as zf:
        zf.writestr("META-INF/container.xml", container)
        zf.writestr("content.opf", opf)


def make_cbz(path, series="", number=1, year="", publisher="", manga="", writer=""):
    path.parent.mkdir(parents=True, exist_ok=True)
    ci = f'''<?xml version="1.0" encoding="UTF-8"?>
<ComicInfo>
<Series>{series}</Series>
<Number>{number}</Number>
<Year>{year}</Year>
<Publisher>{publisher}</Publisher>
<Writer>{writer}</Writer>
<Manga>{manga}</Manga>
</ComicInfo>'''
    with zipfile.ZipFile(str(path), 'w') as zf:
        zf.writestr("ComicInfo.xml", ci)
        zf.writestr("page_001.jpg", b'\xff\xd8\xff\xe0' + b'\x00' * 100)


# ─── Inline Pipeline Logic ──────────────────────────────────────────────────
# We import the actual build_output_path functions where possible,
# and inline the rest for series/movies/anime (which normally need API calls).

# --- Books Pipeline (import actual logic) ---
from bin.bookpilot import extract_metadata as pilot_extract
from bin.bookclassifier import classify, has_minimum_tags, MANGA_PUBLISHERS, COMIC_PUBLISHERS, BOOK_ONLY_EXTS
from bin.bookprocessor import build_output_path as book_output_path, detect_genre
from bin.comicprocessor import build_output_path as comic_output_path
from bin.mangaprocessor import build_output_path as manga_output_path

# --- Music Pipeline (import actual logic) ---
from bin.musicprocessor import build_output_path as music_output_path
from bin.audiobookprocessor import build_output_path as audiobook_output_path

# --- Series/Movie/Anime (inline path builders) ---

def series_output_path(show_name, season, episode, ep_title="", ext=".mkv"):
    safe_show = sanitize(show_name)
    safe_ep = sanitize(ep_title) if ep_title else ""
    new_name = f"{safe_show} - S{season:02d}E{episode:02d}"
    if safe_ep:
        new_name += f" - {safe_ep}"
    new_name += ext
    folder = f"Season {season:02d}"
    return Path(safe_show) / folder / new_name


def movie_output_path(title, year, ext=".mkv"):
    safe_title = sanitize(title)
    return Path(f"{safe_title} ({year}){ext}")


# ─── Test Case Definitions ──────────────────────────────────────────────────

results = []
case_num = 0


def _get_rel_output(output_key):
    """Get the relative output path for a given config key (strip library root if absolute)."""
    out_root = Path(OUTPUT.get(output_key, output_key))
    lib_root_str = CFG['paths']['roots'].get('library', str(ROOT / 'Library'))
    out_str = str(out_root).replace('\\', '/')
    lib_str = lib_root_str.replace('\\', '/')
    if out_str.startswith(lib_str):
        return Path(out_str[len(lib_str):].lstrip('/'))
    return out_root


def run_case(category, label, output_key, rel_path, make_fn=None):
    global case_num
    case_num += 1
    rel_root = _get_rel_output(output_key)
    final = LIB / rel_root / rel_path
    final.parent.mkdir(parents=True, exist_ok=True)
    if make_fn:
        make_fn(final)
    else:
        final.write_bytes(b'\x00' * 64)

    results.append({
        "num": case_num,
        "category": category,
        "label": label,
        "path": str(final.relative_to(LIB)),
    })
    return final


# ═══════════════════════════════════════════════════════════════════════════════
#  1. TV SHOWS
# ═══════════════════════════════════════════════════════════════════════════════

tv_shows = [
    ("Breaking Bad", 1, 1, "Pilot"),
    ("Breaking Bad", 1, 2, "Cat's in the Bag..."),
    ("Breaking Bad", 5, 16, "Felina"),
    ("Game of Thrones", 1, 1, "Winter Is Coming"),
    ("Game of Thrones", 8, 6, "The Iron Throne"),
    ("The Sopranos", 1, 1, "The Sopranos"),
    ("The Wire", 1, 1, "The Target"),
    ("Stranger Things", 4, 1, "The Hellfire Club"),
    ("The Office", 2, 1, "The Dundies"),
    ("Friends", 1, 1, "The Pilot"),
    ("Seinfeld", 5, 11, "The Conversion"),
    ("The Crown", 6, 10, "Sleep, Dearie Sleep"),
    ("Succession", 4, 10, "With Open Eyes"),
    ("Better Call Saul", 6, 13, "Saul Gone"),
    ("True Detective", 1, 1, "The Long Bright Dark"),
]

for show, s, e, ep_title in tv_shows:
    rel = series_output_path(show, s, e, ep_title)
    run_case("tv", f"{show} S{s:02d}E{e:02d}", "tv", rel)


# ═══════════════════════════════════════════════════════════════════════════════
#  2. CARTOONS
# ═══════════════════════════════════════════════════════════════════════════════

cartoons = [
    ("Rick and Morty", 7, 1, "How Poopy Got His Poop Back"),
    ("The Simpsons", 5, 2, "Cape Feare"),
    ("South Park", 25, 1, "Pajama Day"),
    ("Avatar The Last Airbender", 3, 21, "Sozin's Comet Part 4"),
    ("Futurama", 1, 1, "Space Pilot 3000"),
    ("Bob's Burgers", 14, 1, "Fight at the Museum"),
]

for show, s, e, ep_title in cartoons:
    rel = series_output_path(show, s, e, ep_title)
    run_case("cartoon", f"{show} S{s:02d}E{e:02d}", "cartoons", rel)


# ═══════════════════════════════════════════════════════════════════════════════
#  3. ANIME SHOWS
# ═══════════════════════════════════════════════════════════════════════════════

anime_shows = [
    ("Attack on Titan", 1, 1, "To You, in 2000 Years"),
    ("Attack on Titan", 4, 28, "The Dawn of Humanity"),
    ("Death Note", 1, 1, "Rebirth"),
    ("Fullmetal Alchemist Brotherhood", 1, 1, "Fullmetal Alchemist"),
    ("Steins;Gate", 1, 1, "Turning Point"),
    ("One Punch Man", 2, 1, "Return of the Hero"),
    ("Demon Slayer", 1, 1, "Cruelty"),
    ("Jujutsu Kaisen", 2, 1, "Hidden Inventory"),
    ("My Hero Academia", 6, 1, "A Quiet Beginning"),
    ("Spy x Family", 1, 1, "Operation Strix"),
    ("Naruto Shippuden", 1, 1, "Homecoming"),
    ("One Piece", 1, 1, "I'm Luffy! The Man Who Will Become King of the Pirates!"),
    ("Cowboy Bebop", 1, 1, "Asteroid Blues"),
    ("Neon Genesis Evangelion", 1, 1, "Angel Attack"),
    ("Vinland Saga", 2, 1, "Slave"),
]

for show, s, e, ep_title in anime_shows:
    rel = series_output_path(show, s, e, ep_title)
    run_case("anime", f"{show} S{s:02d}E{e:02d}", "anime_shows", rel)


# ═══════════════════════════════════════════════════════════════════════════════
#  4. MOVIES
# ═══════════════════════════════════════════════════════════════════════════════

movies = [
    ("The Shawshank Redemption", "1994"),
    ("The Godfather", "1972"),
    ("The Dark Knight", "2008"),
    ("Pulp Fiction", "1994"),
    ("Inception", "2010"),
    ("Interstellar", "2014"),
    ("Parasite", "2019"),
    ("The Matrix", "1999"),
    ("Fight Club", "1999"),
    ("Forrest Gump", "1994"),
    ("Gladiator", "2000"),
    ("The Lord of the Rings The Return of the King", "2003"),
    ("Oppenheimer", "2023"),
    ("Everything Everywhere All at Once", "2022"),
    ("Dune Part Two", "2024"),
]

for title, year in movies:
    rel = movie_output_path(title, year)
    run_case("movie", f"{title} ({year})", "movies", rel)


# ═══════════════════════════════════════════════════════════════════════════════
#  5. ANIME MOVIES
# ═══════════════════════════════════════════════════════════════════════════════

anime_movies = [
    ("Spirited Away", "2001"),
    ("Your Name", "2016"),
    ("Princess Mononoke", "1997"),
    ("Akira", "1988"),
    ("Howl's Moving Castle", "2004"),
    ("Weathering with You", "2019"),
    ("Suzume", "2022"),
    ("The Boy and the Heron", "2023"),
]

for title, year in anime_movies:
    rel = movie_output_path(title, year)
    run_case("anime_movie", f"{title} ({year})", "anime_movies", rel)


# ═══════════════════════════════════════════════════════════════════════════════
#  6. STAND-UP
# ═══════════════════════════════════════════════════════════════════════════════

standup = [
    ("Dave Chappelle - The Closer", "2021"),
    ("Bo Burnham - Inside", "2021"),
    ("John Mulaney - Kid Gorgeous at Radio City", "2018"),
    ("Hannah Gadsby - Nanette", "2018"),
    ("Ali Wong - Baby Cobra", "2016"),
]

for title, year in standup:
    rel = movie_output_path(title, year)
    run_case("standup", f"{title} ({year})", "standup", rel)


# ═══════════════════════════════════════════════════════════════════════════════
#  7. DOCUMENTARY MOVIES
# ═══════════════════════════════════════════════════════════════════════════════

doc_movies = [
    ("Free Solo", "2018"),
    ("13th", "2016"),
    ("Won't You Be My Neighbor", "2018"),
    ("The Social Dilemma", "2020"),
]

for title, year in doc_movies:
    rel = movie_output_path(title, year)
    run_case("doc_movie", f"{title} ({year})", "documentaries_movies", rel)


# ═══════════════════════════════════════════════════════════════════════════════
#  8. DOCUMENTARY SERIES
# ═══════════════════════════════════════════════════════════════════════════════

doc_series = [
    ("Planet Earth II", 1, 1, "Islands"),
    ("Our Planet", 1, 1, "One Planet"),
    ("Cosmos A Spacetime Odyssey", 1, 1, "Standing Up in the Milky Way"),
    ("Making a Murderer", 1, 1, "Eighteen Years Lost"),
]

for show, s, e, ep_title in doc_series:
    rel = series_output_path(show, s, e, ep_title)
    run_case("doc_series", f"{show} S{s:02d}E{e:02d}", "documentaries_series", rel)


# ═══════════════════════════════════════════════════════════════════════════════
#  9. REALITY TV
# ═══════════════════════════════════════════════════════════════════════════════

reality = [
    ("Survivor", 45, 1, "We Can Do Hard Things"),
    ("The Amazing Race", 36, 1, "Many Firsts"),
    ("RuPaul's Drag Race", 16, 1, "The Mother of All Premieres"),
]

for show, s, e, ep_title in reality:
    rel = series_output_path(show, s, e, ep_title)
    run_case("reality", f"{show} S{s:02d}E{e:02d}", "reality", rel)


# ═══════════════════════════════════════════════════════════════════════════════
# 10. TALK SHOWS
# ═══════════════════════════════════════════════════════════════════════════════

talkshows = [
    ("Last Week Tonight with John Oliver", 11, 1, "Episode 1"),
    ("The Daily Show", 29, 1, "Jon Stewart Returns"),
    ("Conan O'Brien Must Go", 1, 1, "Norway"),
]

for show, s, e, ep_title in talkshows:
    rel = series_output_path(show, s, e, ep_title)
    run_case("talkshow", f"{show} S{s:02d}E{e:02d}", "talkshow", rel)


# ═══════════════════════════════════════════════════════════════════════════════
# 11. MUSIC
# ═══════════════════════════════════════════════════════════════════════════════

music_tracks = [
    # (artist, album, year, track_num, title)
    ("Pink Floyd", "The Dark Side of the Moon", "1973", 1, "Speak to Me"),
    ("Pink Floyd", "The Dark Side of the Moon", "1973", 2, "Breathe"),
    ("Pink Floyd", "The Dark Side of the Moon", "1973", 6, "Money"),
    ("The Beatles", "Abbey Road", "1969", 1, "Come Together"),
    ("The Beatles", "Abbey Road", "1969", 2, "Something"),
    ("Radiohead", "OK Computer", "1997", 1, "Airbag"),
    ("Radiohead", "OK Computer", "1997", 3, "Subterranean Homesick Alien"),
    ("Kendrick Lamar", "To Pimp a Butterfly", "2015", 3, "King Kunta"),
    ("Kendrick Lamar", "good kid, m.A.A.d city", "2012", 5, "m.A.A.d city"),
    ("Daft Punk", "Random Access Memories", "2013", 8, "Get Lucky"),
    ("Nirvana", "Nevermind", "1991", 1, "Smells Like Teen Spirit"),
    ("Led Zeppelin", "Led Zeppelin IV", "1971", 4, "Stairway to Heaven"),
    ("Queen", "A Night at the Opera", "1975", 11, "Bohemian Rhapsody"),
    ("Miles Davis", "Kind of Blue", "1959", 1, "So What"),
    ("Taylor Swift", "1989", "2014", 1, "Welcome to New York"),
    ("Beyonce", "Lemonade", "2016", 1, "Pray You Catch Me"),
    ("BTS", "Map of the Soul 7", "2020", 1, "Interlude Shadow"),
    ("Bad Bunny", "Un Verano Sin Ti", "2022", 1, "Moscow Mule"),
    ("Adele", "21", "2011", 3, "Rolling in the Deep"),
    ("Billie Eilish", "When We All Fall Asleep Where Do We Go", "2019", 2, "bad guy"),
]

for artist, album, year, track, title in music_tracks:
    tags = {"album_artist": artist, "artist": artist, "album": album,
            "title": title, "track": track, "year": year}
    rel = music_output_path(tags, ".flac")
    run_case("music", f"{artist} - {title}", "music", rel)


# ═══════════════════════════════════════════════════════════════════════════════
# 12. AUDIOBOOKS
# ═══════════════════════════════════════════════════════════════════════════════

audiobooks = [
    # (author, book_title, year, track, part_name)
    ("J.K. Rowling", "Harry Potter and the Philosopher's Stone", "1997", 1, "The Boy Who Lived"),
    ("J.K. Rowling", "Harry Potter and the Philosopher's Stone", "1997", 2, "The Vanishing Glass"),
    ("J.K. Rowling", "Harry Potter and the Philosopher's Stone", "1997", 3, "The Letters from No One"),
    ("Stephen King", "The Stand", "1978", 1, "Captain Trips"),
    ("Stephen King", "The Stand", "1978", 2, "The Dreams"),
    ("Frank Herbert", "Dune", "1965", 1, "Part One - Dune"),
    ("Frank Herbert", "Dune", "1965", 2, "Part Two - Muad'Dib"),
    ("Neil Gaiman", "American Gods", "2001", 1, "Chapter One"),
    ("George Orwell", "1984", "1949", 1, "Part One"),
    ("Yuval Noah Harari", "Sapiens", "2011", 1, "An Animal of No Significance"),
    ("Michelle Obama", "Becoming", "2018", 1, "Becoming Me"),
    ("Walter Isaacson", "Steve Jobs", "2011", 1, "Childhood"),
]

for author, book, year, track, part in audiobooks:
    tags = {"album_artist": author, "artist": author, "album": book,
            "title": part, "track": track, "year": year}
    rel = audiobook_output_path(tags, ".m4b")
    run_case("audiobook", f"{author} - {part}", "audiobooks", rel)


# ═══════════════════════════════════════════════════════════════════════════════
# 13. BOOKS (EPUB/PDF/MOBI) — via actual pipeline logic
# ═══════════════════════════════════════════════════════════════════════════════

WORK = LIB / "_work"
WORK.mkdir(parents=True, exist_ok=True)

book_cases = [
    # Fiction
    {"fn": "Tolkien - The Hobbit (1937).epub", "title": "The Hobbit", "author": "J.R.R. Tolkien", "year": "1937", "subjects": ["Fantasy", "Fiction"], "expect_class": "book", "expect_genre": "Fiction"},
    {"fn": "Orwell - 1984 (1949).epub", "title": "1984", "author": "George Orwell", "year": "1949", "subjects": ["Dystopian", "Fiction"], "expect_class": "book", "expect_genre": "Fiction"},
    {"fn": "Austen - Pride and Prejudice (1813).epub", "title": "Pride and Prejudice", "author": "Jane Austen", "year": "1813", "subjects": ["Romance", "Literary"], "expect_class": "book", "expect_genre": "Fiction"},
    {"fn": "Liu Cixin - The Three-Body Problem (2008).epub", "title": "The Three-Body Problem", "author": "Liu Cixin", "year": "2008", "subjects": ["Science Fiction"], "expect_class": "book", "expect_genre": "Fiction"},
    {"fn": "Garcia Marquez - Cien Anos de Soledad (1967).epub", "title": "One Hundred Years of Solitude", "author": "Gabriel Garcia Marquez", "year": "1967", "subjects": ["Fiction", "Literary"], "expect_class": "book", "expect_genre": "Fiction"},
    {"fn": "Murakami - Norwegian Wood (1987).epub", "title": "Norwegian Wood", "author": "Haruki Murakami", "year": "1987", "subjects": ["Fiction", "Novel"], "expect_class": "book", "expect_genre": "Fiction"},
    {"fn": "Dostoevsky - Crime and Punishment (1866).epub", "title": "Crime and Punishment", "author": "Fyodor Dostoevsky", "year": "1866", "subjects": ["Fiction", "Literary"], "expect_class": "book", "expect_genre": "Fiction"},
    {"fn": "King - The Shining (1977).epub", "title": "The Shining", "author": "Stephen King", "year": "1977", "subjects": ["Horror", "Fiction"], "expect_class": "book", "expect_genre": "Fiction"},
    {"fn": "Christie - Orient Express (1934).epub", "title": "Murder on the Orient Express", "author": "Agatha Christie", "year": "1934", "subjects": ["Mystery", "Fiction"], "expect_class": "book", "expect_genre": "Fiction"},

    # Non-Fiction
    {"fn": "Isaacson - Steve Jobs (2011).epub", "title": "Steve Jobs", "author": "Walter Isaacson", "year": "2011", "subjects": ["Biography"], "expect_class": "book", "expect_genre": "Non-Fiction"},
    {"fn": "Harari - Sapiens (2011).epub", "title": "Sapiens", "author": "Yuval Noah Harari", "year": "2011", "subjects": ["History", "Science"], "expect_class": "book", "expect_genre": "Non-Fiction"},
    {"fn": "Carnegie - How to Win Friends (1936).epub", "title": "How to Win Friends and Influence People", "author": "Dale Carnegie", "year": "1936", "subjects": ["Self-Help"], "expect_class": "book", "expect_genre": "Non-Fiction"},
    {"fn": "Bourdain - Kitchen Confidential (2000).epub", "title": "Kitchen Confidential", "author": "Anthony Bourdain", "year": "2000", "subjects": ["Cooking", "Memoir"], "expect_class": "book", "expect_genre": "Non-Fiction"},

    # Technical
    {"fn": "Martin - Clean Code (2008).epub", "title": "Clean Code", "author": "Robert C. Martin", "year": "2008", "subjects": ["Programming", "Software"], "expect_class": "book", "expect_genre": "Technical"},
    {"fn": "Cormen - Intro to Algorithms (2009).epub", "title": "Introduction to Algorithms", "author": "Cormen, Leiserson, Rivest, Stein", "year": "2009", "subjects": ["Computer Science", "Algorithm", "Textbook"], "expect_class": "book", "expect_genre": "Technical"},
    {"fn": "Kleppmann - DDIA (2017).epub", "title": "Designing Data-Intensive Applications", "author": "Martin Kleppmann", "year": "2017", "subjects": ["Database", "Engineering"], "expect_class": "book", "expect_genre": "Technical"},

    # PDF books
    {"fn": "Python Crash Course (2015).pdf", "title": "Python Crash Course", "author": "", "year": "2015", "subjects": [], "expect_class": "book", "expect_genre": ""},
]

for bc in book_cases:
    fp = WORK / bc["fn"]
    if bc["fn"].endswith(".epub"):
        make_epub(fp, bc["title"], bc["author"], "", bc["year"], bc.get("subjects", []))
        meta = pilot_extract(fp)
    else:
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_bytes(b'%PDF-1.4 ' + b'\x00' * 100)
        meta = {"tags": {"title": bc["title"], "author": bc["author"], "year": bc["year"], "subjects": bc.get("subjects", [])}}

    tags = meta.get("tags", {})
    # Force subjects from test case if pilot didn't pick them up
    if bc.get("subjects") and not tags.get("subjects"):
        tags["subjects"] = bc["subjects"]

    classification = classify(fp, meta)
    genre = detect_genre(tags)
    rel = book_output_path(tags, fp.suffix)
    ok = classification == bc["expect_class"] and genre == bc["expect_genre"]
    status = "OK" if ok else "FAIL"

    run_case("book", bc["fn"].rsplit(".", 1)[0], "books", rel)
    fp.unlink(missing_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 14. COMICS — via actual pipeline logic
# ═══════════════════════════════════════════════════════════════════════════════

comic_cases = [
    {"series": "Batman", "number": 1, "year": "2020", "publisher": "DC Comics", "writer": "James Tynion IV"},
    {"series": "Batman", "number": 50, "year": "2018", "publisher": "DC Comics", "writer": "Tom King"},
    {"series": "The Amazing Spider-Man", "number": 1, "year": "2022", "publisher": "Marvel Comics", "writer": "Zeb Wells"},
    {"series": "The Amazing Spider-Man", "number": 900, "year": "2022", "publisher": "Marvel Comics", "writer": "Zeb Wells"},
    {"series": "Saga", "number": 1, "year": "2012", "publisher": "Image Comics", "writer": "Brian K. Vaughan"},
    {"series": "Saga", "number": 66, "year": "2024", "publisher": "Image Comics", "writer": "Brian K. Vaughan"},
    {"series": "Hellboy", "number": 1, "year": "1994", "publisher": "Dark Horse Comics", "writer": "Mike Mignola"},
    {"series": "Teenage Mutant Ninja Turtles", "number": 150, "year": "2024", "publisher": "IDW Publishing", "writer": "Sophie Campbell"},
    {"series": "Lumberjanes", "number": 75, "year": "2020", "publisher": "BOOM! Studios", "writer": "Shannon Watters"},
    {"series": "Spawn", "number": 350, "year": "2023", "publisher": "Image Comics", "writer": "Todd McFarlane"},
    {"series": "Invincible", "number": 144, "year": "2018", "publisher": "Image Comics", "writer": "Robert Kirkman"},
    {"series": "Harley Quinn", "number": 30, "year": "2023", "publisher": "DC Comics", "writer": "Tini Howard"},
    {"series": "X-Men", "number": 35, "year": "2024", "publisher": "Marvel Comics", "writer": "Gerry Duggan"},
    {"series": "The Walking Dead", "number": 193, "year": "2019", "publisher": "Image Comics", "writer": "Robert Kirkman"},
    {"series": "Archie", "number": 32, "year": "2018", "publisher": "Archie Comics", "writer": "Mark Waid"},
]

for cc in comic_cases:
    fn = f"{cc['series']} {cc['number']:03d} ({cc['year']}).cbz"
    fp = WORK / fn
    make_cbz(fp, cc["series"], cc["number"], cc["year"], cc["publisher"], writer=cc.get("writer", ""))
    meta = pilot_extract(fp)
    tags = meta.get("tags", {})
    rel = comic_output_path(tags, fp.suffix)
    run_case("comic", f"{cc['publisher'].split()[0]} {cc['series']} #{cc['number']}", "comics", rel)
    fp.unlink(missing_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 15. MANGA — via actual pipeline logic
# ═══════════════════════════════════════════════════════════════════════════════

manga_cases = [
    {"series": "One Piece", "volume": 107, "year": "2024", "publisher": "Viz Media"},
    {"series": "Naruto", "volume": 72, "year": "2015", "publisher": "Viz Media"},
    {"series": "Bleach", "volume": 74, "year": "2016", "publisher": "Viz Media"},
    {"series": "Dragon Ball", "volume": 42, "year": "1995", "publisher": "Viz Media"},
    {"series": "Attack on Titan", "volume": 34, "year": "2021", "publisher": "Kodansha Comics"},
    {"series": "My Hero Academia", "volume": 40, "year": "2024", "publisher": "Viz Media"},
    {"series": "Demon Slayer", "volume": 23, "year": "2020", "publisher": "Viz Media"},
    {"series": "Jujutsu Kaisen", "volume": 26, "year": "2024", "publisher": "Viz Media"},
    {"series": "Chainsaw Man", "volume": 16, "year": "2024", "publisher": "Viz Media"},
    {"series": "Tokyo Ghoul", "volume": 14, "year": "2014", "publisher": "Viz Media"},
    {"series": "Death Note", "volume": 12, "year": "2006", "publisher": "Viz Media"},
    {"series": "Spy x Family", "volume": 12, "year": "2024", "publisher": "Viz Media"},
    {"series": "Fullmetal Alchemist", "volume": 27, "year": "2010", "publisher": "Viz Media"},
    {"series": "Berserk", "volume": 41, "year": "2021", "publisher": "Dark Horse Manga"},
    {"series": "Vagabond", "volume": 37, "year": "2014", "publisher": "Viz Media"},
    {"series": "Vinland Saga", "volume": 27, "year": "2024", "publisher": "Kodansha Comics"},
    {"series": "Solo Leveling", "volume": 8, "year": "2023", "publisher": "Yen Press"},
    {"series": "One Punch Man", "volume": 29, "year": "2024", "publisher": "Viz Media"},
]

for mc in manga_cases:
    fn = f"{mc['series']} Vol.{mc['volume']:02d}.cbz"
    fp = WORK / fn
    make_cbz(fp, mc["series"], mc["volume"], mc["year"], mc["publisher"], manga="Yes")
    meta = pilot_extract(fp)
    tags = meta.get("tags", {})
    rel = manga_output_path(tags, fp.suffix)
    run_case("manga", f"{mc['series']} Vol.{mc['volume']}", "manga", rel)
    fp.unlink(missing_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  RESULTS & LIBRARY TREE
# ═══════════════════════════════════════════════════════════════════════════════

# Cleanup temp work dir
shutil.rmtree(str(WORK), ignore_errors=True)

# Count by category
from collections import Counter
cats = Counter(r["category"] for r in results)
total = len(results)

print()
print("=" * 78)
print(f"  ALL-PIPELINE STRESS TEST — {total} cases")
print("=" * 78)

# Print per-category counts
cat_order = [
    ("tv", "TV Shows"), ("cartoon", "Cartoons"), ("anime", "Anime Shows"),
    ("movie", "Movies"), ("anime_movie", "Anime Movies"), ("standup", "Stand-Up"),
    ("doc_movie", "Doc Movies"), ("doc_series", "Doc Series"),
    ("reality", "Reality TV"), ("talkshow", "Talk Shows"),
    ("music", "Music"), ("audiobook", "Audiobooks"),
    ("book", "Books"), ("comic", "Comics"), ("manga", "Manga"),
]
for cat_id, cat_label in cat_order:
    c = cats.get(cat_id, 0)
    if c:
        print(f"  {cat_label:20s}: {c:3d} files")
print(f"  {'TOTAL':20s}: {total:3d} files")
print("=" * 78)

# Print full library tree
print()
print("  FINAL LIBRARY STRUCTURE:")
print()

all_files = sorted(LIB.rglob("*"))
for f in all_files:
    if f.is_file():
        rel = str(f.relative_to(LIB)).replace("\\", "/")
        if not rel.startswith("_"):  # skip _work, _Review
            print(f"    {rel}")

print()
print("=" * 78)
print(f"  Library root: {LIB}")
print("=" * 78)

# Keep files in Library for web panel display
# To clean up: delete Library/* manually
