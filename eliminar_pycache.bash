#!/usr/bin/env bash

echo "Searching and deleting __pycache__ directories (excluding venv)..."

find . -path '*/venv' -prune -o -type d -name "__pycache__" -exec sh -c 'echo "Deleting: $1"; rm -rf "$1"' _ {} \;

echo "Deletion complete."