#!/bin/bash
# ──────────────────────────────────────────────────────────────
# MyMediaManager - Bare-Metal Installer
# Usage: curl -fsSL https://raw.githubusercontent.com/Sachinlearnsdev/MyMediaManager/main/install.sh | sudo bash
# ──────────────────────────────────────────────────────────────
set -e

REPO_URL="https://github.com/Sachinlearnsdev/MyMediaManager.git"
INSTALL_DIR="/opt/mymediamanager"
SERVICE_NAME="mymediamanager"
MMM_USER="mmm"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[mmm]${NC} $1"; }
warn()  { echo -e "${YELLOW}[mmm]${NC} $1"; }
error() { echo -e "${RED}[mmm]${NC} $1"; exit 1; }

# ──────────────────────────────────────────────
# Pre-flight checks
# ──────────────────────────────────────────────
if [ "$(id -u)" -ne 0 ]; then
    error "This installer must be run as root (sudo)"
fi

info "MyMediaManager Installer"
echo ""

# Check Python 3.8+
if ! command -v python3 &>/dev/null; then
    error "Python 3 is required. Install it first: apt install python3 python3-pip python3-venv"
fi

PY_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)
if [ "$PY_MAJOR" -lt 3 ] || { [ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -lt 8 ]; }; then
    error "Python 3.8+ required, found $PY_VERSION"
fi
info "Python $PY_VERSION detected"

# ──────────────────────────────────────────────
# Install system dependencies
# ──────────────────────────────────────────────
info "Installing system dependencies..."

if command -v apt-get &>/dev/null; then
    apt-get update -qq
    apt-get install -y -qq git unrar p7zip-full python3-venv > /dev/null 2>&1 || {
        warn "unrar not found in main repo, trying non-free..."
        add-apt-repository -y non-free 2>/dev/null || true
        sed -i 's/^Components: main$/Components: main non-free/' /etc/apt/sources.list.d/debian.sources 2>/dev/null || true
        apt-get update -qq
        apt-get install -y -qq git unrar p7zip-full python3-venv > /dev/null 2>&1
    }
elif command -v dnf &>/dev/null; then
    dnf install -y -q git unrar p7zip python3-pip > /dev/null 2>&1
elif command -v pacman &>/dev/null; then
    pacman -Sy --noconfirm --quiet git unrar p7zip python > /dev/null 2>&1
else
    warn "Unknown package manager. Please install manually: git, unrar, p7zip"
fi

# ──────────────────────────────────────────────
# Create service user
# ──────────────────────────────────────────────
if ! id "$MMM_USER" &>/dev/null; then
    info "Creating service user: $MMM_USER"
    useradd -r -m -s /bin/bash "$MMM_USER"
fi

# ──────────────────────────────────────────────
# Download / update application
# ──────────────────────────────────────────────
if [ -d "$INSTALL_DIR/.git" ]; then
    info "Updating existing installation..."
    cd "$INSTALL_DIR"
    sudo -u "$MMM_USER" git pull --ff-only || {
        warn "Git pull failed, continuing with existing version"
    }
else
    info "Downloading MyMediaManager..."
    git clone "$REPO_URL" "$INSTALL_DIR"
    chown -R "$MMM_USER":"$MMM_USER" "$INSTALL_DIR"
fi

# ──────────────────────────────────────────────
# Python virtual environment
# ──────────────────────────────────────────────
info "Setting up Python environment..."
VENV_DIR="$INSTALL_DIR/venv"

if [ ! -d "$VENV_DIR" ]; then
    sudo -u "$MMM_USER" python3 -m venv "$VENV_DIR"
fi

sudo -u "$MMM_USER" "$VENV_DIR/bin/pip" install --quiet --upgrade pip > /dev/null 2>&1
sudo -u "$MMM_USER" "$VENV_DIR/bin/pip" install --quiet -r "$INSTALL_DIR/requirements.txt" > /dev/null 2>&1

# ──────────────────────────────────────────────
# App directories
# ──────────────────────────────────────────────
info "Setting up application..."
mkdir -p "$INSTALL_DIR"/{cache,logs,config}
mkdir -p "$INSTALL_DIR"/cache/{tv,anime,movies,cartoons,classifier,reality,talkshow,documentaries}
chown -R "$MMM_USER":"$MMM_USER" "$INSTALL_DIR"

# ──────────────────────────────────────────────
# Generate default config (if first install)
# Data root and Library root left blank —
# user configures via Settings > Paths.
# ──────────────────────────────────────────────
CONFIG_FILE="$INSTALL_DIR/config/config.json"
if [ ! -f "$CONFIG_FILE" ]; then
    info "Generating default config..."
    cat > "$CONFIG_FILE" << CONFIGEOF
{
  "api_config": {
    "anime": { "search": ["mal","jikan","anilist"], "episode_titles": ["jikan","anilist"], "sequel_chain": ["mal","jikan"] },
    "tv": { "search": ["tvdb"], "episode_titles": ["tvdb"] },
    "cartoons": { "search": ["tvdb"], "episode_titles": ["tvdb"] },
    "reality": { "search": ["tvdb"], "episode_titles": ["tvdb"] },
    "talkshow": { "search": ["tvdb"], "episode_titles": ["tvdb"] },
    "documentaries": { "search": ["tvdb"], "episode_titles": ["tvdb"] },
    "movies": { "search": ["tmdb"] },
    "classifier": { "anime_check": ["mal","jikan"], "tv_check": ["tvdb"] }
  },
  "api_keys": {},
  "cache": { "api_cache_ttl_hours": 24, "show_cache_ttl_hours": 720, "root": "cache", "show_cache_file": "cache/show_cache.json", "noise_learned_file": "cache/learned_noise.json" },
  "flow_control": { "series_quota_gb": 100, "movies_quota_gb": 100, "min_ssd_free_gb": 10 },
  "logging": { "level": "INFO", "path": "logs", "retention_sessions": 5 },
  "paths": {
    "roots": { "manager": "$INSTALL_DIR", "data": "", "library": "" },
    "series_pipeline": {
      "input_drop": "Drop_Shows", "system_drop": ".Work/Shows/Intake", "system_home": ".Work/Shows",
      "processing": ".Work/Shows/Processing", "failed": ".Work/Shows/Failed",
      "review": "Review/Shows", "duplicates": "Duplicates/Shows",
      "staged": { "identify": ".Work/Shows/Staged/Identify", "tv": ".Work/Shows/Staged/TV_Shows", "cartoons": ".Work/Shows/Staged/Cartoons", "anime": ".Work/Shows/Staged/Anime", "reality": ".Work/Shows/Staged/Reality", "talkshow": ".Work/Shows/Staged/TalkShows", "documentaries": ".Work/Shows/Staged/Documentaries" }
    },
    "movie_pipeline": {
      "input_drop": "Drop_Movies", "system_drop": ".Work/Movies/Intake", "system_home": ".Work/Movies",
      "processing": ".Work/Movies/Processing", "failed": ".Work/Movies/Failed",
      "review": "Review/Movies", "duplicates": "Duplicates/Movies",
      "staged": { "movies": ".Work/Movies/Staged" }
    },
    "output": { "tv": "TV Shows", "cartoons": "Cartoons", "anime_shows": "Anime/Shows", "anime_movies": "Anime/Movies", "movies": "Movies", "reality": "Reality TV", "talkshow": "Talk Shows", "documentaries_series": "Documentaries/Series", "documentaries_movies": "Documentaries/Movies", "standup": "Stand-Up" },
    "trash_root": "Trash"
  },
  "tuning": { "scan_interval": 5, "extract_timeout": 300, "mouse_scan_interval": 2, "mouse_stability_scans": 4, "mouse_batch_settle": 2, "confidence_classifier": 40, "confidence_tv": 60, "confidence_cartoon": 60, "confidence_movie": 75, "trash_max_age_days": 7, "review_max_age_days": 14 },
  "web_panel": { "port": 8888, "host": "0.0.0.0", "session_timeout_hours": 24, "password_hash": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918", "api_key": "" }
}
CONFIGEOF
    chown "$MMM_USER":"$MMM_USER" "$CONFIG_FILE"
fi

# Create .env if missing
if [ ! -f "$INSTALL_DIR/.env" ]; then
    cat > "$INSTALL_DIR/.env" << 'ENVEOF'
# MyMediaManager API Keys
# Add your keys here or configure them via the web panel at Settings > API Keys
#
# MMM_TVDB_KEY=
# MMM_TMDB_KEY=
# MMM_MAL_KEY=
ENVEOF
    chown "$MMM_USER":"$MMM_USER" "$INSTALL_DIR/.env"
    chmod 600 "$INSTALL_DIR/.env"
fi

# ──────────────────────────────────────────────
# Create systemd service
# ──────────────────────────────────────────────
info "Creating systemd service..."

cat > /etc/systemd/system/${SERVICE_NAME}.service << SERVICEEOF
[Unit]
Description=MyMediaManager - Automated Media Organization
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=$MMM_USER
Group=$MMM_USER
WorkingDirectory=$INSTALL_DIR
ExecStart=$VENV_DIR/bin/python $INSTALL_DIR/webpanel.py
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

NoNewPrivileges=true
ProtectSystem=false
ReadWritePaths=/

[Install]
WantedBy=multi-user.target
SERVICEEOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME" > /dev/null 2>&1

# ──────────────────────────────────────────────
# Start the service
# ──────────────────────────────────────────────
info "Starting MyMediaManager..."
systemctl start "$SERVICE_NAME"

sleep 2

SERVER_IP=$(hostname -I | awk '{print $1}')

if systemctl is-active --quiet "$SERVICE_NAME"; then
    echo ""
    info "────────────────────────────────────────────"
    info " MyMediaManager is running!"
    info ""
    info " Web Panel:     http://${SERVER_IP}:8888"
    info " Default login: admin / admin"
    info ""
    info " Next steps:"
    info "   1. Open the web panel and change your password"
    info "   2. Go to Settings > Paths and set your data & library roots"
    warn "      Data root and Library root must be on the same drive!"
    info "   3. Go to Settings > API Keys and add your TVDB + TMDB keys"
    info "   4. Go to Dashboard and Start All"
    info ""
    info " Commands:"
    info "   Status:    systemctl status $SERVICE_NAME"
    info "   Logs:      journalctl -u $SERVICE_NAME -f"
    info "   Stop:      systemctl stop $SERVICE_NAME"
    info "   Restart:   systemctl restart $SERVICE_NAME"
    info "   Uninstall: sudo $INSTALL_DIR/uninstall.sh"
    info "────────────────────────────────────────────"
else
    error "Service failed to start. Check: journalctl -u $SERVICE_NAME -e"
fi
