#!/bin/sh
set -e

CONFIG_DIR="/home/ergosphere/.config/ergosphere"
CONFIG_FILE="$CONFIG_DIR/config.toml"

mkdir -p "$CONFIG_DIR"

if [ -f "$CONFIG_FILE" ]; then
    echo "[+] Custom configuration file detected via volume mount."
    chown ergosphere:ergosphere "$CONFIG_FILE"
else
    echo "[+] No configuration file found. Provisioning empty baseline for environment mirroring..."
    touch "$CONFIG_FILE"
    chown ergosphere:ergosphere "$CONFIG_FILE"
fi

exec "$@"