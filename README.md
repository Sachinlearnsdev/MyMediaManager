# MyMediaManager

An automated quad-pipeline media engine that organizes TV shows, movies, music, audiobooks, eBooks, comics, and manga into a structured Plex/Jellyfin/Calibre-compatible library.

## Features

- **31 automated services** across 4 pipelines (Series, Movies, Music, Books)
- **15 output categories**: TV Shows, Cartoons, Anime (Shows & Movies), Reality TV, Talk Shows, Documentaries (Series & Movies), Stand-Up, Movies, Music, Audiobooks, Books, Comics, Manga
- **Smart classification** using MAL, TVDB, TMDB, AniList, MusicBrainz, AcoustID, Google Books, OpenLibrary, ComicVine
- **Confidence scoring** with fuzzy matching, show cache, and per-type thresholds
- **Web control panel** with real-time pipeline visualization, log viewer, and service management
- **Review system** for low-confidence matches across all pipelines
- **Duplicate detection** with size/quality comparison and replace/delete options
- **Noise learner** that progressively cleans filenames from learned patterns
- **Docker & bare metal** deployment with one-command install

## Pipeline Flow

```
Drop_Shows / Drop_Movies / Drop_Music / Drop_Books
     |
  AutoMouse -----> File stability monitoring & batch settling
     |
  AutoHarbor ----> Archive extraction (RAR/ZIP/7z), CBR→CBZ conversion
     |
  AutoRouter ----> Extension routing + junk filtering
     |
  StructPilot / MusicPilot / BookPilot ---> Metadata extraction
     |
  ContentClassifier / AudioClassifier / BookClassifier ---> Smart routing
     |
  Final Processors (API-driven metadata enrichment)
     |
  Organized Library (Plex/Jellyfin/Calibre ready)
```

## Quick Start

### Docker (Recommended)

```bash
git clone https://github.com/Sachinlearnsdev/MyMediaManager.git
cd MyMediaManager/docker
docker compose up -d
```

Web panel at **http://localhost:8888** — login: `admin` / `admin`

### Bare Metal (Linux)

```bash
curl -fsSL https://raw.githubusercontent.com/Sachinlearnsdev/MyMediaManager/main/install.sh | sudo bash
```

### Manual

```bash
git clone https://github.com/Sachinlearnsdev/MyMediaManager.git
cd MyMediaManager
pip install -r requirements.txt
cp config/config.template.json config/config.json
python webpanel.py
```

### After Install

1. Open the web panel and change your password
2. Go to **Settings > Paths** and set your data & library roots (must be on the same drive)
3. Go to **Settings > API Keys** and add your keys (TVDB + TMDB minimum for video)
4. Go to **Dashboard** and click **Start All**

## Web Panel

- **Dashboard**: Horizontal swimlane pipeline visualization, collapsible lanes, file token tracking, review & duplicates management
- **Logs**: Real-time log viewer with search, filters, pipeline grouping, and session history
- **Settings**: Paths, API keys, priority chains, flow control quotas, confidence thresholds
- **Statistics**: Per-processor stats, cache hit rates, Top Shows/Artists/Authors, library overview
- **Library**: Browse all 15 media categories with search, "new" badges, and file details
- **Recovery**: Flush stuck files, retry failed/review, nuclear reset, cache management
- **Noise Learner**: View, approve, reject, and manually add filename cleaning patterns
- **Guide**: Interactive setup checklist with API key status and drop folder mapping

## API Keys

| API | Used For | Required |
|-----|----------|----------|
| TVDB | TV shows, cartoons, reality, talk shows, docs | Yes (video) |
| TMDB | Movies | Yes (video) |
| MAL | Anime detection & metadata | Recommended |
| MusicBrainz | Music metadata | Free (no key) |
| AcoustID | Audio fingerprinting | Free |
| Google Books | Book metadata | Free (no key) |
| OpenLibrary | Book metadata fallback | Free (no key) |
| ComicVine | Comic metadata | Free |
| AniList | Anime episodes, manga | Free (no key) |
| Trakt, OMDb, Fanart, IGDB | Additional metadata | Optional |

Keys are stored in `.env` (gitignored). The web panel manages them automatically via **Settings > API Keys**.

## Configuration

Edit `config/config.json` for paths, tuning parameters, and API priority ordering. See `config/config.template.json` for the full structure.

## Requirements

- Python 3.10+
- System: `unrar`, `p7zip-full` (for archive extraction)
- Docker: just Docker + Docker Compose

## License

MIT
