#!/bin/sh

if [ "$DEBUG" = "1" ]; then
  echo "Starting in debug mode (waiting for debugger)..."
  python -m debugpy --listen 0.0.0.0:5678 --wait-for-client -m uvicorn main:app --host 0.0.0.0 --port 8000
else
  uvicorn main:app --host 0.0.0.0 --port 8000 --reload
fi
