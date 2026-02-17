#!/usr/bin/env bash

set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-my_base}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PG_FILE="${SCRIPT_DIR}/database_postgis.sql"
MG_FILE="${SCRIPT_DIR}/database_mongodb.py"

if [[ ! -r "$PG_FILE" ]]; then
    echo "[!] PostGIS file not found or not readable: $PG_FILE" >&2
    exit 1
fi
if [[ ! -r "$MG_FILE" ]]; then
    echo "[!] MongoDB file not found or not readable: $MG_FILE" >&2
    exit 1
fi

echo "[INFO] Usage:"
echo "  PGHOST=$PGHOST  PGPORT=$PGPORT  PGUSER=$PGUSER  PGDATABASE=$PGDATABASE"
echo "  SQL: $PG_FILE"
echo "  PY : $MG_FILE"
echo

# Helper
db_exist() {
    PGPASSWORD="${PGPASSWORD:-}" psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -tAc \
      "SELECT 1 FROM pg_database WHERE datname='${PGDATABASE}'" | grep -q 1
}

# PostGIS
echo "[INFO] ----- PostgreSQL / PostGIS -----"

if ! db_exist; then
    echo "[INFO] DB Creation '$PGDATABASE'"
    PGPASSWORD="${PGPASSWORD:-}" createdb -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$PGDATABASE"
else
    echo "[INFO] DB '$PGDATABASE' already present"
fi

echo "[INFO] CREATE EXTENSION postgis;"
PGPASSWORD="${PGPASSWORD:-}" psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
  -c "CREATE EXTENSION IF NOT EXISTS postgis;"
echo "[INFO] Executing PostGIS seed (stdin redirection to avoid read permissions)"
PGPASSWORD="${PGPASSWORD:-}" psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
  -f "$PG_FILE"

echo "[OK] PostGIS seed completed"
echo

# MongoDB
echo "[INFO] ----- MongoDB -----"

echo "[INFO] Running the MongoDB seed"
python3 "$MG_FILE"

echo "[OK] MongoDB seed completed"

echo
echo "[DONE] Both databases have been (re)created and seeded."