#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 https://github.com/OWNER/calr-guard.git"
  exit 1
fi

REMOTE_URL="$1"

if [ ! -d .git ]; then
  git init -b main
  git add .
  git commit -m "Initial commit: CalR Guard 0.3.1"
fi

if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "$REMOTE_URL"
else
  git remote add origin "$REMOTE_URL"
fi

git push -u origin main
