#!/bin/bash
set -e

# ──────────────────────────────────────────────
# PUID/PGID - Match host user for file permissions
# ──────────────────────────────────────────────
PUID=${PUID:-1000}
PGID=${PGID:-1000}

if [ "$(id -u mmm)" != "$PUID" ] || [ "$(id -g mmm)" != "$PGID" ]; then
    echo "[mmm] Setting user mmm to UID=$PUID GID=$PGID"
    groupmod -o -g "$PGID" mmm 2>/dev/null || true
    usermod -o -u "$PUID" -g mmm mmm 2>/dev/null || true
fi

# ──────────────────────────────────────────────
# First-run: Generate default config
# ──────────────────────────────────────────────
if [ ! -f /app/config/config.json ]; then
    echo "[mmm] First run detected - generating default config..."
    cp /defaults/config.docker.json /app/config/config.json
fi

# First-run: Create empty .env for API keys
if [ ! -f /app/config/.env ]; then
    echo "[mmm] Creating empty .env for API keys..."
    cat > /app/config/.env << 'ENVFILE'
# MyMediaManager API Keys
# Add your keys here or configure them via the web panel at Settings > API Keys
#
# MMM_TVDB_KEY=
# MMM_TMDB_KEY=
# MMM_MAL_KEY=
# MMM_TRAKT_KEY=
# MMM_OMDB_KEY=
# MMM_FANART_KEY=
# MMM_IGDB_KEY=
ENVFILE
fi

# Symlink .env so the app finds it at project root (/app/.env)
if [ ! -L /app/.env ] && [ -f /app/config/.env ]; then
    ln -sf /app/config/.env /app/.env
fi

# ──────────────────────────────────────────────
# App directories (always needed)
# ──────────────────────────────────────────────
mkdir -p /app/cache/tv /app/cache/anime /app/cache/movies
mkdir -p /app/cache/cartoons /app/cache/classifier
mkdir -p /app/cache/reality /app/cache/talkshow /app/cache/documentaries
mkdir -p /app/logs

# ──────────────────────────────────────────────
# Pipeline directories (if paths are configured)
# Runs as root so we can create dirs + fix perms
# on any mounted volume regardless of ownership
# ──────────────────────────────────────────────
DATA_ROOT=$(python3 -c "
import json
try:
    cfg = json.load(open('/app/config/config.json'))
    print(cfg.get('paths',{}).get('roots',{}).get('data',''))
except: pass
" 2>/dev/null)

LIBRARY_ROOT=$(python3 -c "
import json
try:
    cfg = json.load(open('/app/config/config.json'))
    print(cfg.get('paths',{}).get('roots',{}).get('library',''))
except: pass
" 2>/dev/null)

if [ -n "$DATA_ROOT" ] && [ -d "$(dirname "$DATA_ROOT")" ]; then
    echo "[mmm] Creating pipeline directories in $DATA_ROOT..."
    mkdir -p "$DATA_ROOT/Drop_Shows" "$DATA_ROOT/Drop_Movies" "$DATA_ROOT/Trash"
    mkdir -p "$DATA_ROOT/Review/Shows" "$DATA_ROOT/Review/Movies"
    mkdir -p "$DATA_ROOT/Duplicates/Shows" "$DATA_ROOT/Duplicates/Movies"
    mkdir -p "$DATA_ROOT/.Work/Shows/Intake"
    mkdir -p "$DATA_ROOT/.Work/Shows/Processing"
    mkdir -p "$DATA_ROOT/.Work/Shows/Failed"
    mkdir -p "$DATA_ROOT/.Work/Shows/Staged/Identify"
    mkdir -p "$DATA_ROOT/.Work/Shows/Staged/TV_Shows"
    mkdir -p "$DATA_ROOT/.Work/Shows/Staged/Cartoons"
    mkdir -p "$DATA_ROOT/.Work/Shows/Staged/Anime"
    mkdir -p "$DATA_ROOT/.Work/Shows/Staged/Reality"
    mkdir -p "$DATA_ROOT/.Work/Shows/Staged/TalkShows"
    mkdir -p "$DATA_ROOT/.Work/Shows/Staged/Documentaries"
    mkdir -p "$DATA_ROOT/.Work/Movies/Intake"
    mkdir -p "$DATA_ROOT/.Work/Movies/Processing"
    mkdir -p "$DATA_ROOT/.Work/Movies/Failed"
    mkdir -p "$DATA_ROOT/.Work/Movies/Staged"
    chown -R mmm:mmm "$DATA_ROOT"
fi

if [ -n "$LIBRARY_ROOT" ] && [ -d "$(dirname "$LIBRARY_ROOT")" ]; then
    echo "[mmm] Creating library directories in $LIBRARY_ROOT..."
    mkdir -p "$LIBRARY_ROOT/TV Shows"
    mkdir -p "$LIBRARY_ROOT/Movies"
    mkdir -p "$LIBRARY_ROOT/Anime/Shows"
    mkdir -p "$LIBRARY_ROOT/Anime/Movies"
    mkdir -p "$LIBRARY_ROOT/Cartoons"
    mkdir -p "$LIBRARY_ROOT/Reality TV"
    mkdir -p "$LIBRARY_ROOT/Talk Shows"
    mkdir -p "$LIBRARY_ROOT/Documentaries/Series"
    mkdir -p "$LIBRARY_ROOT/Documentaries/Movies"
    mkdir -p "$LIBRARY_ROOT/Stand-Up"
    chown -R mmm:mmm "$LIBRARY_ROOT"
fi

# ──────────────────────────────────────────────
# Fix ownership (app dir)
# ──────────────────────────────────────────────
chown -R mmm:mmm /app
chown -h mmm:mmm /app/.env 2>/dev/null || true

# ──────────────────────────────────────────────
# Startup banner
# ──────────────────────────────────────────────
SERVER_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
SERVER_IP=${SERVER_IP:-localhost}

echo "[mmm] ──────────────────────────────────────────"
echo "[mmm]  MyMediaManager starting"
echo "[mmm]"
echo "[mmm]  Web Panel:    http://${SERVER_IP}:8888"
echo "[mmm]  Default login: admin / admin"
echo "[mmm]"
if [ -z "$DATA_ROOT" ] || [ -z "$LIBRARY_ROOT" ]; then
echo "[mmm]  Next steps:"
echo "[mmm]    1. Open the web panel and change your password"
echo "[mmm]    2. Go to Settings > Paths and set your data & library roots"
echo "[mmm]       Data + Library must be on the same drive!"
echo "[mmm]    3. Go to Settings > API Keys and add your TVDB + TMDB keys"
echo "[mmm]    4. Go to Dashboard and Start All"
else
echo "[mmm]  Data root:    $DATA_ROOT"
echo "[mmm]  Library root: $LIBRARY_ROOT"
fi
echo "[mmm]"
echo "[mmm]  Commands:"
echo "[mmm]    Logs:     docker compose logs -f"
echo "[mmm]    Stop:     docker compose down"
echo "[mmm]    Restart:  docker compose restart"
echo "[mmm] ──────────────────────────────────────────"

# ──────────────────────────────────────────────
# Run as mmm user
# ──────────────────────────────────────────────
exec gosu mmm "$@"
