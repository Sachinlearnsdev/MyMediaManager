FROM python:3.11-slim-bookworm

LABEL maintainer="MyMediaManager"
LABEL description="Automated media organization pipeline for Plex/Jellyfin"

# ──────────────────────────────────────────────
# System dependencies: unrar (full RAR5), 7z
# ──────────────────────────────────────────────
RUN sed -i 's/^Components: main$/Components: main non-free/' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        unrar \
        p7zip-full \
        tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ──────────────────────────────────────────────
# Python dependencies (cached layer)
# ──────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ──────────────────────────────────────────────
# Application code
# ──────────────────────────────────────────────
COPY bin/ bin/
COPY web/ web/
COPY webpanel.py .
COPY config/config.template.json config/config.template.json
COPY docker/config.docker.json /defaults/config.docker.json
COPY docker/docker-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Make app dirs writable by PUID user (default 1000)
RUN chown -R 1000:1000 /app /defaults

# ──────────────────────────────────────────────
# Volumes
# ──────────────────────────────────────────────
#   /app/config  - config.json + .env (API keys)
#   /app/cache   - show cache, API caches, noise learner
#   /app/logs    - per-service log files
#   /data        - pipeline working dirs (Drop_Shows, Drop_Movies, Work, Trash, Review, Duplicates)
#   /library     - final organized media (TV Shows, Movies, Anime, etc.)
VOLUME ["/app/config", "/app/cache", "/app/logs", "/data", "/library"]

EXPOSE 8888

ENTRYPOINT ["tini", "--", "/entrypoint.sh"]
CMD ["python", "webpanel.py"]
