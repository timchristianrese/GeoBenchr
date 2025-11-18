#!/bin/bash
set -e

# === Base Dependencies ===
sudo apt update
sudo apt-get install -y ssh pdsh openjdk-11-jdk maven git openssh-server wget libxml2-utils make g++ libsnappy1v5 </dev/null

# === Java Setup ===
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# === Passwordless SSH for Hadoop ===
if [ ! -f ~/.ssh/id_rsa ]; then
  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
fi
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# === ZooKeeper ===
ZOOKEEPER_VERSION="3.9.4"
sudo wget "https://downloads.apache.org/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz"
sudo tar -xvf apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
sudo mv apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper

cat > /opt/zookeeper/conf/zoo.cfg <<EOF
tickTime=2000
dataDir=/opt/zookeeper/data
clientPort=2181
initLimit=5
syncLimit=2
standaloneEnabled=true
admin.enableServer=false
EOF

/opt/zookeeper/bin/zkServer.sh start

# === Hadoop ===
HADOOP_VERSION="3.4.0"
wget "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
tar -xvf hadoop-${HADOOP_VERSION}.tar.gz
sudo mv hadoop-${HADOOP_VERSION} /opt/hadoop

mkdir -p /opt/hadoop/namenode /opt/hadoop/datanode /opt/hadoop/tmp

# Add Hadoop to PATH
echo "export HADOOP_HOME=/opt/hadoop" >> ~/.bashrc
echo "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin" >> ~/.bashrc
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Hadoop configs
cat > /opt/hadoop/etc/hadoop/hadoop-env.sh <<EOF
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PDSH_RCMD_TYPE=ssh
EOF

cat > /opt/hadoop/etc/hadoop/core-site.xml <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/opt/hadoop/tmp</value>
  </property>
</configuration>
EOF

cat > /opt/hadoop/etc/hadoop/hdfs-site.xml <<EOF
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:/opt/hadoop/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:/opt/hadoop/datanode</value>
  </property>
</configuration>
EOF

cat > /opt/hadoop/etc/hadoop/yarn-site.xml <<EOF
<configuration>
  <property>
    <name>yarn.acl.enable</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.admin.acl</name>
    <value>*</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>localhost</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>512</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>4096</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>8192</value>
  </property>
</configuration>
EOF

# === Accumulo ===
ACCUMULO_VERSION="2.1.4"
wget -q https://dlcdn.apache.org/accumulo/${ACCUMULO_VERSION}/accumulo-${ACCUMULO_VERSION}-bin.tar.gz
tar -xvf accumulo-${ACCUMULO_VERSION}-bin.tar.gz
sudo mv accumulo-${ACCUMULO_VERSION} /opt/accumulo

echo "export ACCUMULO_HOME=/opt/accumulo" >> ~/.bashrc
echo "export ZOOKEEPER_HOME=/opt/zookeeper" >> ~/.bashrc
export ACCUMULO_HOME=/opt/accumulo
export ZOOKEEPER_HOME=/opt/zookeeper
ACCUMULO_LOG_DIR=/opt/accumulo/logs
mkdir -p $ACCUMULO_LOG_DIR

cat > /opt/accumulo/conf/accumulo-env.sh <<EOF
# Java and Hadoop settings
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
JAVA_OPTS="-Xms512m -Xmx1g -XX:+UseParallelGC"

# Paths
ACCUMULO_HOME=/opt/accumulo
ZOOKEEPER_HOME=/opt/zookeeper
HADOOP_HOME=/opt/hadoop

# Classpath
CLASSPATH=$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*

MALLOC_ARENA_MAX=4
export MALLOC_ARENA_MAX
ACCUMULO_LOG_DIR=$ACCUMULO_HOME/logs
EOF

# Minimal accumulo.properties
cat > /opt/accumulo/conf/accumulo.properties <<EOF
instance.volumes=hdfs://localhost:9000/accumulo
instance.zookeepers=localhost:2181
EOF

# accumulo-client.properties
cat > /opt/accumulo/conf/accumulo-client.properties <<EOF
instance.name=test
instance.zookeepers=localhost:2181
auth.principal=root
auth.token=test
EOF

/opt/accumulo/bin/accumulo-util build-native
/opt/accumulo/bin/accumulo-cluster create-config

# === GeoMesa ===
TAG="5.3.0"
SCALA_VERSION="2.12"
VERSION="2.12-${TAG}"
wget -q "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-accumulo_${VERSION}-bin.tar.gz"
tar xvf geomesa-accumulo_${VERSION}-bin.tar.gz
sudo mv geomesa-accumulo_${VERSION} /opt/geomesa-accumulo
sudo mv /opt/geomesa-accumulo/dist/accumulo/geomesa-accumulo-distributed-runtime_${SCALA_VERSION}-${TAG}.jar /opt/accumulo/lib

echo "export GEOMESA_ACCUMULO_HOME=/opt/geomesa-accumulo" >> ~/.bashrc
echo "export PATH=\$PATH:\$GEOMESA_ACCUMULO_HOME/bin" >> ~/.bashrc
export GEOMESA_ACCUMULO_HOME=/opt/geomesa-accumulo
export PATH=$PATH:$GEOMESA_ACCUMULO_HOME/bin

# === Final Initialization ===

# Format HDFS
$HADOOP_HOME/bin/hdfs namenode -format -force

# Change Permissions of Hadoop log directory
sudo chmod 777 $HADOOP_HOME/logs
# Start Hadoop
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Initialize Accumulo instance
$ACCUMULO_HOME/bin/accumulo init --instance-name test -u root --password test

echo "✅ Installation complete. Run '/opt/accumulo/bin/accumulo-cluster start' to start Accumulo."
