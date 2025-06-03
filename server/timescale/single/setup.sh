#!/bin/bash

set -e

echo "=== Installing TimeScaleDB and PostGIS on Ubuntu ==="

# Step 1: Update packages
sudo apt-get update && sudo apt-get upgrade -y

# Step 2: Add PostgreSQL APT repository
echo "Adding PostgreSQL APT repository..."
sudo apt-get install -y wget gnupg lsb-release
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
RELEASE=$(lsb_release -cs)
echo "deb http://apt.postgresql.org/pub/repos/apt/ $RELEASE-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list

# Step 3: Add TimeScaleDB PPA
echo "Adding TimeScaleDB PPA..."
echo "deb https://packagecloud.io/timescale/timescaledb/ubuntu/ $RELEASE main" | sudo tee /etc/apt/sources.list.d/timescaledb.list
curl -L https://packagecloud.io/timescale/timescaledb/gpgkey | sudo apt-key add -

# Step 4: Update repositories again
sudo apt-get update

# Step 5: Install PostgreSQL with TimeScaleDB and PostGIS
echo "Installing PostgreSQL, TimeScaleDB, and PostGIS..."
sudo apt-get install -y postgresql-15 \
                        timescaledb-2-postgresql-15 \
                        postgresql-15-postgis-3 \
                        postgresql-15-postgis-3-scripts

# Step 6: Configure TimeScaleDB
echo "Configuring TimeScaleDB..."
sudo timescaledb-tune --quiet --yes

# Step 7: Restart PostgreSQL
echo "Restarting PostgreSQL..."
sudo systemctl restart postgresql

# Step 8: Create a test database and enable extensions
echo "Setting up test database with extensions..."
sudo -u postgres psql <<EOF
CREATE DATABASE spatiotemporal;
\c spatiotemporal
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;
EOF

echo "Installation complete. TimeScaleDB with PostGIS is ready!"
