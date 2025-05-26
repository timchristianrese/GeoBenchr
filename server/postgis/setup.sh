#!/bin/bash
set -e

echo "Installing PostgreSQL 14 and PostGIS..."
sudo apt update
sudo apt install -y postgresql-14 postgresql-server-dev-14 \
    postgresql-14-postgis-3 postgresql-14-postgis-3-scripts

echo "Creating database and enabling PostGIS..."
sudo -u postgres psql -c "CREATE DATABASE postgisdb;"
sudo -u postgres psql -d postgisdb -c "CREATE EXTENSION postgis;"

echo "PostGIS is installed and ready (Database: postgisdb)"
