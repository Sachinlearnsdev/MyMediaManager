#!/bin/bash
# ──────────────────────────────────────────────
# MyMediaManager - Uninstaller
# ──────────────────────────────────────────────
set -e

SERVICE_NAME="mymediamanager"
INSTALL_DIR="/opt/mymediamanager"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[mmm]${NC} $1"; }
warn()  { echo -e "${YELLOW}[mmm]${NC} $1"; }

if [ "$(id -u)" -ne 0 ]; then
    echo -e "${RED}[mmm]${NC} Run as root (sudo)"
    exit 1
fi

echo ""
warn "This will stop and remove MyMediaManager."
warn "Your media library and data directories will NOT be deleted."
echo ""
read -p "Continue? [y/N] " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    info "Cancelled."
    exit 0
fi

if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
    info "Stopping service..."
    systemctl stop "$SERVICE_NAME"
fi

if systemctl is-enabled --quiet "$SERVICE_NAME" 2>/dev/null; then
    systemctl disable "$SERVICE_NAME" > /dev/null 2>&1
fi

if [ -f "/etc/systemd/system/${SERVICE_NAME}.service" ]; then
    info "Removing systemd service..."
    rm "/etc/systemd/system/${SERVICE_NAME}.service"
    systemctl daemon-reload
fi

if [ -d "$INSTALL_DIR" ]; then
    info "Removing application files..."
    rm -rf "$INSTALL_DIR"
fi

info "────────────────────────────────────────────"
info " MyMediaManager has been uninstalled."
info ""
info " Your data and library directories are preserved."
info " Delete them manually if no longer needed."
info "────────────────────────────────────────────"
