#!/bin/sh
set -eu

role="${1:-collector}"

case "$role" in
  source)
    exec python -m uvicorn mock_source.app:app --host 0.0.0.0 --port 9000
    ;;
  collector)
    exec python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
    ;;
  *)
    exec "$@"
    ;;
esac
