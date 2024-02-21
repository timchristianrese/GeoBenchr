sudo apt update
sudo apt install openjdk-8-jdk -y
sudo apt install maven -y
java -version

# prereqs needed for accept certs
apt install ca-certificates gnupg -y
sudo apt install postgresql -y
sudo apt-get -y install postgresql-16-cron 
"listen_addresses = '*'" >> /etc/postgresql/*/main/postgresql.conf


curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg >/dev/null
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'


# to allow dependencies to be installed
cat << EOF >> /etc/apt/preferences.d/pgdg.pref
Package: *
Pin: release o=apt.postgresql.org
Pin-Priority: 500
EOF

sudo apt update
sudo apt upgrade


sudo apt install -y postgresql-16-postgis-3
sudo apt install postgresql-16-pgrouting

#geomesa
export TAG="4.0.1"
export VERSION="2.12-${TAG}" # note: 2.12 is the Scala build version

wget "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-gt_${VERSION}-bin.tar.gz"
tar xvf geomesa-gt_${VERSION}-bin.tar.gz
cd geomesa-gt_${VERSION}
./bin/install-shapefile-support.sh
pg_lsclusters


