#!/bin/bash
set -e

# ──────────────────────────────────────────────
# PUID/PGID — run as the user's UID/GID
# Same pattern as Sonarr/Radarr/Jellyfin
# ──────────────────────────────────────────────
PUID=${PUID:-1000}
PGID=${PGID:-1000}

if ! getent group mmm > /dev/null 2>&1; then
    groupadd -g "$PGID" mmm 2>/dev/null || groupadd mmm
fi
if ! getent passwd mmm > /dev/null 2>&1; then
    useradd -u "$PUID" -g mmm -s /bin/bash -M mmm 2>/dev/null || useradd -g mmm -s /bin/bash -M mmm
fi

chown -R mmm:mmm /app /defaults

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
mkdir -p /app/cache/music /app/cache/audiobooks /app/cache/books /app/cache/comics /app/cache/manga
mkdir -p /app/logs

# ──────────────────────────────────────────────
# Startup banner
# ──────────────────────────────────────────────
SERVER_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
SERVER_IP=${SERVER_IP:-localhost}

echo "[mmm] ──────────────────────────────────────────"
echo "[mmm]  MyMediaManager starting"
echo "[mmm]  Running as UID=${PUID} GID=${PGID}"
echo "[mmm]"
echo "[mmm]  Web Panel:    http://${SERVER_IP}:8888"
echo "[mmm]  Default login: admin / admin"
echo "[mmm]"
echo "[mmm]  Next steps:"
echo "[mmm]    1. Open the web panel and change your password"
echo "[mmm]    2. Go to Settings > Paths and set your data & library roots"
echo "[mmm]       Data + Library must be on the same drive!"
echo "[mmm]    3. Go to Settings > API Keys and add your TVDB + TMDB keys"
echo "[mmm]    4. Go to Dashboard and Start All"
echo "[mmm]"
echo "[mmm]  Commands:"
echo "[mmm]    Logs:     docker compose logs -f"
echo "[mmm]    Stop:     docker compose down"
echo "[mmm]    Restart:  docker compose restart"
echo "[mmm] ──────────────────────────────────────────"

# Drop from root to mmm (PUID:PGID) and run the app
exec gosu mmm "$@"
