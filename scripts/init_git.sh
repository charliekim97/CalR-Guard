#!/usr/bin/env bash
set -euo pipefail

git init -b main
git add .
git commit -m "Initial commit: CalR Guard 0.3.1"

echo "Local git repository initialized on branch main."
echo "Next: add your GitHub remote and push. See docs/PUBLISH_TO_GITHUB.md"
