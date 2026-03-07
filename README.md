# MyMediaManager

A twin-engine automated media pipeline that organizes downloaded TV shows, anime, cartoons, reality TV, talk shows, documentaries, stand-up specials, and movies into a structured Plex/Jellyfin-compatible library.

## Features

- **16 automated services** processing files through a multi-stage pipeline
- **10 output categories**: TV Shows, Cartoons, Anime, Reality TV, Talk Shows, Documentaries (Series & Movies), Stand-Up, Movies, Anime Movies
- **Smart classification** using MAL, TVDB, TMDB, AniList, and other APIs
- **Confidence scoring** with fuzzy matching and show cache
- **Web control panel** with real-time monitoring, log viewer, and service management
- **Review system** for low-confidence matches with manual override
- **Duplicate detection** with size comparison and replace/delete options
- **Dry run testing** to preview classification without moving files

## Pipeline Flow

```
Drop_Shows / Drop_Movies
     |
  AutoMouse -----> File stability monitoring
     |
  AutoHarbor ----> Archive extraction (RAR/ZIP/7z)
     |
  AutoRouter ----> Extension routing + junk filtering
     |
  StructPilot ---> Filename normalization
     |
  [Series] ContentClassifier --> Routes to anime/tv/cartoons/reality/talkshow/docs
     |
  Final Processors (7 services with API-driven metadata)
     |
  Organized Library (Plex/Jellyfin ready)
```

## Quick Start

### Requirements

- Python 3.10+
- API keys: TVDB, TMDB, MAL (minimum)
- Optional: Trakt, OMDb, Fanart, IGDB

### Installation

```bash
# Clone
git clone https://github.com/Sachinlearnsdev/MyMediaManager.git
cd MyMediaManager

# Install dependencies
pip install -r requirements.txt

# Configure API keys
cp config/config.template.json config/config.json
# Create .env file with API keys:
# MMM_TVDB_KEY=your_key
# MMM_TMDB_KEY=your_key
# MMM_MAL_KEY=your_key

# Start the web panel
python webpanel.py

# Or start the full pipeline (Linux)
chmod +x mymediamanager.sh
./mymediamanager.sh
```

The web panel runs on port **8888** by default. First login: `admin` / `admin` (you'll be prompted to change it).

## Web Panel

- **Dashboard**: Service control, pipeline flow visualization, review/duplicates management
- **Logs**: Real-time log viewer with search, filters, and session history
- **Settings**: Paths, API keys, priority configuration, tuning parameters
- **Statistics**: Processing stats, cache browser, library overview
- **Library**: Browse organized media with "new" detection
- **Recovery**: Pipeline tools for stuck files and cache management

## Configuration

API keys are stored in `.env` (gitignored), not in config.json. The web panel manages both automatically.

Edit `config/config.json` for paths, tuning parameters, and API priority ordering. See `config/config.template.json` for the full structure.

## License

MIT
