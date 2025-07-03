sudo apt install gnupg postgresql-common apt-transport-https lsb-release wget
sudo apt install postgresql-server-dev-17
echo "deb https://packagecloud.io/timescale/timescaledb/debian/ $(lsb_release -c -s) main" | sudo tee /etc/apt/sources.list.d/timescaledb.list
wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/timescaledb.gpg
sudo apt update
sudo apt install timescaledb-2-postgresql-17
sudo timescaledb-tune
sudo systemctl restart postgresql