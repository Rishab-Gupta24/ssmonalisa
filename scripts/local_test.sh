#!/bin/bash
# Local loop without Docker: build natively, run all fixtures, then the dev set.
# Usage: scripts/local_test.sh [DEV_DIR] [ANSWER_DIR]
set -e
R="$(cd "$(dirname "$0")/.." && pwd)"
DEV="${1:-$HOME/ss/dev}"
ANSWER="${2:-$HOME/ss/dev-answer}"
PY="${PY:-$HOME/ssenv/bin/python}"
mkdir -p "$HOME/ss/bin"
g++ -std=c++20 -O3 -march=x86-64-v3 -pthread -Wall -o "$HOME/ss/bin/ss" "$R/src/main.cpp"
export NCPU="${NCPU:-8}"
"$PY" "$R/scripts/run_fixtures.py" "$R/fixtures" -- "$HOME/ss/bin/ss" | tail -${TAILN:-40}
if [ -d "$DEV" ]; then
  rm -rf "$HOME/ss/out" && mkdir -p "$HOME/ss/out"
  /usr/bin/time -f "dev run: %e s, %M KB max RSS" env DATA_DIR="$DEV" OUTPUT_DIR="$HOME/ss/out" "$HOME/ss/bin/ss"
  (cd "$R/scripts" && "$PY" compare.py "$HOME/ss/out" "$ANSWER")
fi
