#!/usr/bin/env bash
set -eo pipefail

echo "[mymediamanager] Initializing MyMediaManager Stack..."

# Derive ROOT from script location (no hardcoding)
ROOT="$(cd "$(dirname "$0")" && pwd)"
BIN="$ROOT/bin"
CONFIG_FILE="$ROOT/config/config.json"

# Read paths from config using Python (single source of truth)
read_config() {
    python3 -c "
import json
cfg = json.load(open('$CONFIG_FILE'))
roots = cfg['paths']['roots']
manager = roots['manager']
# Data root: where all file I/O happens. Defaults to manager root.
# Set this to your HDD so pipeline moves are instant (same filesystem).
data = roots.get('data', manager)
library = roots.get('library', data + '/Library')
print(manager)
print(data)
print(library)
"
}

CONFIG_VALUES=$(read_config)
MANAGER_ROOT=$(echo "$CONFIG_VALUES" | sed -n '1p')
STORAGE_ROOT=$(echo "$CONFIG_VALUES" | sed -n '2p')
LIBRARY_ROOT=$(echo "$CONFIG_VALUES" | sed -n '3p')

# PIDs array to track all background processes
PIDS=()

# ------------------------------------------------------------------------------
# 1. Self-Healing Infrastructure
# ------------------------------------------------------------------------------
init_infrastructure() {
    echo "[mymediamanager] Enforcing Folder Structure..."

    # 1. Input Drops (User accessible)
    mkdir -p "$STORAGE_ROOT/Drop_Shows"
    mkdir -p "$STORAGE_ROOT/Drop_Movies"
    mkdir -p "$STORAGE_ROOT/Trash"
    mkdir -p "$STORAGE_ROOT/Review"

    # 2. Work Queues (Internal Pipeline - hidden with dot prefix)
    # Series Pipeline
    mkdir -p "$STORAGE_ROOT/.Work/Shows/Intake"
    mkdir -p "$STORAGE_ROOT/.Work/Shows/Processing"
    mkdir -p "$STORAGE_ROOT/.Work/Shows/Failed"
    mkdir -p "$STORAGE_ROOT/.Work/Shows/Staged/Identify"
    mkdir -p "$STORAGE_ROOT/.Work/Shows/Staged/TV_Shows"
    mkdir -p "$STORAGE_ROOT/.Work/Shows/Staged/Cartoons"
    mkdir -p "$STORAGE_ROOT/.Work/Shows/Staged/Anime"
    mkdir -p "$STORAGE_ROOT/.Work/Shows/Staged/Reality"
    mkdir -p "$STORAGE_ROOT/.Work/Shows/Staged/TalkShows"
    mkdir -p "$STORAGE_ROOT/.Work/Shows/Staged/Documentaries"

    # Movie Pipeline
    mkdir -p "$STORAGE_ROOT/.Work/Movies/Intake"
    mkdir -p "$STORAGE_ROOT/.Work/Movies/Processing"
    mkdir -p "$STORAGE_ROOT/.Work/Movies/Failed"
    mkdir -p "$STORAGE_ROOT/.Work/Movies/Staged"

    # 3. Output Libraries
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

    # 4. System Caches & Logs (manager root / SSD)
    mkdir -p "$MANAGER_ROOT/logs"
    mkdir -p "$MANAGER_ROOT/cache/tv"
    mkdir -p "$MANAGER_ROOT/cache/anime"
    mkdir -p "$MANAGER_ROOT/cache/movies"
    mkdir -p "$MANAGER_ROOT/cache/cartoons"
    mkdir -p "$MANAGER_ROOT/cache/classifier"
    mkdir -p "$MANAGER_ROOT/cache/reality"
    mkdir -p "$MANAGER_ROOT/cache/talkshow"
    mkdir -p "$MANAGER_ROOT/cache/documentaries"

    # Set Permissions (*arr pattern: shared media group)
    REAL_USER="${MMM_USER:-$(whoami)}"
    REAL_GROUP="${MMM_GROUP:-media}"
    echo "[mymediamanager] Setting ownership to: $REAL_USER:$REAL_GROUP"
    chown -R "$REAL_USER":"$REAL_GROUP" "$STORAGE_ROOT" "$LIBRARY_ROOT" "$MANAGER_ROOT/cache" "$MANAGER_ROOT/logs" 2>/dev/null || true
    chmod -R 775 "$STORAGE_ROOT" "$LIBRARY_ROOT" 2>/dev/null || true
}

# ------------------------------------------------------------------------------
# 2. Service Launcher Helper
# ------------------------------------------------------------------------------
launch() {
    local script="$1"
    local mode="$2"

    local cmd="$BIN/$script"

    if [[ ! -f "$cmd" ]]; then
        echo "[mymediamanager][ERROR] Missing script: $cmd"
        exit 1
    fi

    if [[ "$mode" == "standalone" ]]; then
        echo "[mymediamanager] Starting $script (Standalone)"
        python3 "$cmd" &
    elif [[ "$script" == *"seriesprocessor"* ]]; then
        echo "[mymediamanager] Starting $script (Type: $mode)"
        python3 "$cmd" --type "$mode" &
    else
        echo "[mymediamanager] Starting $script (Mode: $mode)"
        python3 "$cmd" --mode "$mode" &
    fi

    PIDS+=($!)
    sleep 0.5
}

# ------------------------------------------------------------------------------
# 3. Shutdown Logic
# ------------------------------------------------------------------------------
stop_services() {
    echo "[mymediamanager] Stopping all services..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait || true
    echo "[mymediamanager] Shutdown complete."
}

trap stop_services SIGINT SIGTERM

# ------------------------------------------------------------------------------
# 4. Main Execution
# ------------------------------------------------------------------------------

# Step A: Build Infrastructure
init_infrastructure

# Step B: Start Series Pipeline
launch "automouse.py" "series"
launch "autoharbor.py" "series"
launch "autorouter.py" "series"
launch "structpilot.py" "series"
launch "contentclassifier.py" "series"

# Step C: Start Movie Pipeline
launch "automouse.py" "movies"
launch "autoharbor.py" "movies"
launch "autorouter.py" "movies"
launch "structpilot.py" "movies"
launch "movieprocessor.py" "movies"

# Step D: Start Final Processors
launch "seriesprocessor.py" "tv"
launch "seriesprocessor.py" "cartoons"
launch "seriesprocessor.py" "reality"
launch "seriesprocessor.py" "talkshow"
launch "seriesprocessor.py" "documentaries"
launch "animeprocessor.py" "standalone"

# Keep script running
wait
