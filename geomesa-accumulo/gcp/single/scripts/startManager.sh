sudo apt update
sudo apt-get install -y --force-yes ssh pdsh openjdk-11-jdk maven git openssh-server wget libxml2-utils make g++ libsnappy1v5 openjdk-8-jdk </dev/null

echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc

#setup passwordless ssh for Hadoop
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

#install zookeeper
wget https://downloads.apache.org/zookeeper/zookeeper-3.9.2/apache-zookeeper-3.9.2-bin.tar.gz
tar -xvf apache-zookeeper-3.9.2-bin.tar.gz
sudo mv apache-zookeeper-3.9.2-bin /opt/zookeeper
cd /opt/zookeeper
cp conf/zoo_sample.cfg conf/zoo.cfg
bin/zkServer.sh start

#Optional, check if it worked
#bin/zkCli.sh -server 127.0.0.1:2181

#install hadoop
cd ~
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz
tar -xvf hadoop-3.4.0.tar.gz
sudo mv hadoop-3.4.0 /opt/hadoop
cd /opt/hadoop
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> etc/hadoop/hadoop-env.sh
#optional, test if works
bin/hadoop -version

echo "<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>" > etc/hadoop/core-site.xml

echo "<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>" > etc/hadoop/hdfs-site.xml

echo "export PDSH_RCMD_TYPE=ssh" >> etc/hadoop/hadoop-env.sh
echo "export PDSH_RCMD_TYPE=ssh" >> ~/.bashrc
export PDSH_RCMD_TYPE=ssh

bin/hdfs namenode -format
sbin/start-dfs.sh

#install accumulo 
cd ~
wget https://dlcdn.apache.org/accumulo/2.1.3/accumulo-2.1.3-bin.tar.gz
tar -xvf accumulo-2.1.3-bin.tar.gz
sudo mv accumulo-2.1.3 /opt/accumulo
cd /opt/accumulo
echo "JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> conf/accumulo-env.sh
bin/accumulo-util build-native
export ACCUMULO_HOME=/opt/accumulo
export ZOOKEEPER_HOME=/opt/zookeeper
export HADOOP_HOME=/opt/hadoop
sed -i '/ZOOKEEPER_HOME=/a\
ACCUMULO_HOME=/opt/accumulo\
ZOOKEEPER_HOME=/opt/zookeeper\
HADOOP_HOME=/opt/hadoop' conf/accumulo-env.sh

#replace 8020 in accumulo.properties with 9000
sed -i 's/8020/9000/g' conf/accumulo.properties
#replace instance_name= in accumulo-client.properties with instance_name=test
sed -i 's/instance.name=/instance.name=test/g' conf/accumulo-client.properties
sed -i 's/auth.principal=/auth.principal=root/g' conf/accumulo-client.properties
sed -i 's/auth.token=/auth.token=test/g' conf/accumulo-client.properties



cd ~
export TAG="5.0.1"
export SCALA_VERSION="2.12"
export VERSION="2.12-${TAG}" # note: 2.12 is the Scala build version
# download and unpackage the most recent distribution:
wget "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-accumulo_${VERSION}-bin.tar.gz"
tar xvf geomesa-accumulo_${VERSION}-bin.tar.gz
sudo mv geomesa-accumulo_${VERSION} /opt/geomesa-accumulo 
sudo mv /opt/geomesa-accumulo/dist/accumulo/geomesa-accumulo-distributed-runtime_${SCALA_VERSION}-${TAG}.jar /opt/accumulo/lib

cd /opt/accumulo
bin/accumulo init --instance-name test -u root --password test
bin/accumulo-cluster create-config
#make services remotely reachable instead of just from localhost
sed -i "s/localhost/0.0.0.0/g" conf/cluster.yaml
ssh-keyscan 0.0.0.0 >> ~/.ssh/known_hosts
bin/accumulo-cluster start