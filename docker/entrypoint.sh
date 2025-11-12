#!/usr/bin/env bash
set -e
MODE="${1:-webui}"
if [ -z "$(ls -A "$CONFIG_DIR" 2>/dev/null)" ]; then
  mkdir -p "$CONFIG_DIR"
fi
mkdir -p "$DATA_DIR"
if [ "$MODE" = "webui" ]; then
  exec uvicorn src.m3u_merge.webapp:app --host 0.0.0.0 --port 8188
elif [ "$MODE" = "once" ]; then
  exec python -m src.m3u_merge.cli run --full
else
  exec "$@"
fi
