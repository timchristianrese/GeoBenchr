#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TST_DIR="${SCRIPT_DIR}/"
QR_DIR="tstqueries"
PY_FILE="${SCRIPT_DIR}/../script/generateQueries.py"

[[ -f "$PY_FILE" ]] || { echo "[!] File not found: $PY_FILE" >&2; exit 1; }
[[ -d "$TST_DIR" ]] || { echo "[ERR] Directory not found: $TST_DIR" >&2; exit 1; }

echo "Deleting test query files in: $QR_DIR"
rm -rf "${QR_DIR}"

echo "Generating test queries..."
python3 "${PY_FILE}" -pm -r -o "$QR_DIR" "$TST_DIR"

echo "[OK] Output in: $QR_DIR"