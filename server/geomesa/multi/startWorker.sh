sudo apt update
sudo apt-get install -y --force-yes ssh pdsh openjdk-11-jdk maven git openssh-server wget libxml2-utils make g++ libsnappy1v5 openjdk-8-jdk </dev/null

echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc

#setup passwordless ssh for Hadoop
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

#install zookeeper
export ZOOKEEPER_VERSION="3.9.3"
wget https://downloads.apache.org/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
tar -xvf apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
sudo mv apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper
cd /opt/zookeeper
cp conf/zoo_sample.cfg conf/zoo.cfg
#no need to run it, as it will be run in the manager script
#bin/zkServer.sh start


#install hadoop
cd ~
export HADOOP_VERSION="3.4.0"
wget https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
tar -xvf hadoop-${HADOOP_VERSION}.tar.gz
sudo mv hadoop-${HADOOP_VERSION} /opt/hadoop
cd /opt/hadoop
mkdir data
mkdir log-dir
mkdir local-dir
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> etc/hadoop/hadoop-env.sh
#optional, test if works


echo "<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://accumulo-namenode-manager:9000</value>
    </property>
    <property>
        <name>io.file.buffer.size</name>
        <value>131072</value>
    </property>
</configuration>" > etc/hadoop/core-site.xml
echo "<configuration>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///opt/hadoop/data</value>
    </property>
</configuration>" > etc/hadoop/hdfs-site.xml

#set properties for NodeManager
echo "<configuration>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>-1</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-pmem-ratio</name>
        <value>2.1</value>
    </property>
    <property>
        <name>yarn.nodemanager.local-dirs</name>
        <value>/opt/hadoop/local-dir</value>
    </property>
    <property>
        <name>yarn.nodemanager.log-dirs</name>
        <value>/opt/hadoop/log-dir</value>
    </property>
    <property>
        <name>yarn.nodemanager.log.retain-seconds</name>
        <value>10800</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
    </property>
</configuration>" > etc/hadoop/yarn-site.xml

#configure mapred-site.xml
echo "<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>1536</value>
    </property>
    <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>3072</value>
    </property>
    <property>
        <name>mapreduce.map.java.opts</name>
        <value>-Xmx1228m</value>
    </property>
    <property>
        <name>mapreduce.reduce.java.opts</name>
        <value>-Xmx2457m</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=/opt/hadoop</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=/opt/hadoop</value>
    </property>
</configuration>" > etc/hadoop/mapred-site.xml
echo "export PDSH_RCMD_TYPE=ssh" >> etc/hadoop/hadoop-env.sh
echo "export PDSH_RCMD_TYPE=ssh" >> ~/.bashrc
export PDSH_RCMD_TYPE=ssh

#write worker to file for hadoop, based on the number passed in $1
((WORKER_COUNT = $1 - 1))
for i in $(seq 0 $WORKER_COUNT); do
    machine_name="accumulo-worker-$i"
    #if i is zero, write to masters file, else write to workers file
    if [ $i -eq 0 ]; then
        echo "$machine_name" > etc/hadoop/workers
    else
        echo "$machine_name" >> etc/hadoop/workers
    fi
    ip_address=$(nslookup $machine_name | awk '/^Address: / { print $2 }')
# Check if the machine name and IP address already exist in /etc/hosts
    if ! grep -q "$ip_address $machine_name" /etc/hosts; then
        # If they don't exist, append them to /etc/hosts
        echo "$ip_address $machine_name" | sudo tee -a /etc/hosts
    fi
done
machine_name="accumulo-namenode-manager"
ip_address=$(nslookup $machine_name | awk '/^Address: / { print $2 }')
if ! grep -q "$ip_address $machine_name" /etc/hosts; then
    echo "$ip_address $machine_name" | sudo tee -a /etc/hosts
fi

cd ~
export ACCUMULO_VERSION="2.1.3"
wget https://dlcdn.apache.org/accumulo/${ACCUMULO_VERSION}/accumulo-${ACCUMULO_VERSION}-bin.tar.gz
tar -xvf accumulo-${ACCUMULO_VERSION}-bin.tar.gz
sudo mv accumulo-${ACCUMULO_VERSION} /opt/accumulo
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
sed -i 's/localhost/accumulo-namenode-manager/g' conf/accumulo.properties
#replace instance_name= in accumulo-client.properties with instance_name=test
sed -i 's/instance.name=/instance.name=test/g' conf/accumulo-client.properties
sed -i 's/auth.principal=/auth.principal=root/g' conf/accumulo-client.properties
sed -i 's/auth.token=/auth.token=test/g' conf/accumulo-client.properties
sed -i 's/instance.zookeepers=localhost/instance.zookeepers=accumulo-namenode-manager/g' conf/accumulo-client.properties
bin/accumulo-cluster create-config
#bin/accumulo init --instance-name test --password test --clear-instance-name


# Generate the replacement string
replacement=""
for ((i=0; i<$1; i++))
do
  replacement+="  - accumulo-worker-$i\n"
done

# Use sed to replace the text in the file
sed -i '/tserver:$/N;s|tserver:\n  - localhost|tserver:\n'"$replacement"'|' conf/cluster.yaml
sed -i 's/localhost/accumulo-namenode-manager/g' conf/cluster.yaml

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
#bin/accumulo init --instance-name test -u root --password test
#make services remotely reachable instead of just from localhost
#bin/accumulo-cluster start