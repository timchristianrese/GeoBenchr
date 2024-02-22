sudo apt update
sudo apt install openjdk-8-jdk -y
sudo apt install maven -y
java -version

#hadoop
sudo apt-get install pdsh -y
cd 
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar xzf hadoop-3.3.6.tar.gz
printf "export JAVA_HOME=/usr/java/latest" >> etc/hadoop/hadoop-env.sh
sudo ./hadoop-3.3.6/hadoop/bin/start-all.sh

#zookeeper
useradd zookeeper -m
usermod --shell /bin/bash zookeeper
usermod -aG sudo zookeeper
sudo mkdir -p /data/zookeeper
chown -R zookeeper:zookeeper /data/zookeeper
cd /opt
wget https://dlcdn.apache.org/zookeeper/zookeeper-3.9.1/apache-zookeeper-3.9.1-bin.tar.gz
sudo tar -xvf apache-zookeeper-3.6.1-bin.tar.gz
mv apache-zookeeper-3.6.1-bin zookeeper
chown -R zookeeper:zookeeper /opt/zookeeper
printf "tickTime = 2000\ndataDir = /data/zookeeper\nclientPort = 2181\ninitLimit = 5\nsyncLimit = 2\n" >> /opt/zookeeper/conf/zoo.cfg
sudo bin/zkServer.sh start

#accumulo
wget https://dlcdn.apache.org/accumulo/2.1.2/accumulo-2.1.2-src.tar.gz
tar xzf accumulo-2.1.2-bin.tar.gz
cd accumulo-2.1.2
sudo ./bin/start_all.sh  