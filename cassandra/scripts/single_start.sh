sudo apt update
sudo apt install openjdk-8-jdk -y
java -version
sudo apt install apt-transport-https
echo "deb https://debian.cassandra.apache.org 41x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
curl https://downloads.apache.org/cassandra/KEYS | sudo apt-key add -
sudo apt-get update
sudo apt-get install cassandra
sudo apt install maven
cqlsh -e "CREATE KEYSPACE geo WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 1};"
export CASSANDRA_HOME="/usr/share/cassandra/lib"
wget "https://github.com/locationtech/geomesa/releases/download/geomesa-4.0.5/geomesa-cassandra_2.12-4.0.5-bin.tar.gz"
tar xvf geomesa-cassandra_2.12-4.0.5-bin.tar.gz
cd geomesa-cassandra_2.12-4.0.5/
./bin/install-dependencies.sh
./bin/install-shapefile-support.sh
bin/geomesa-cassandra

