#!/bin/bash

# Exit on error
SSH_USER="tim"
#set IP to default value if not provided
IP="${1:-10.35.0.22}"  # FOO will be assigned 'default' value if VARIABLE not set or null.
# The value of VARIABLE remains untouched.
echo $IP
#setup machine to local ssh to itself and others (todo second part)
ssh "$SSH_USER@$IP" << 'EOF'
cd ~/.ssh 

# Generate a public/private rsa key pair; 
# Use the default options
ssh-keygen -t rsa -q -f "$HOME/.ssh/id_rsa" -N ""

# Append the key to the authorized_keys file
cat id_rsa.pub >> authorized_keys

# Set the required permissions 
sudo chmod 640 authorized_keys

# Restart service with the latest changes (keys)
sudo service ssh restart
EOF


ssh "$SSH_USER@$IP" << 'EOF'
#!/bin/bash

set -e

echo "🔧 Updating package lists..."
sudo apt update

echo "🔧 Installing Java, Maven, Git, and other dependencies..."
sudo apt install -y openjdk-11-jdk maven git wget unzip

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc

SPARK_VERSION="3.5.6"
HADOOP_VERSION="3"
SCALA_VERSION="2.12"
SEDONA_VERSION="1.7.1"

echo "Downloading Apache Spark ${SPARK_VERSION}... if need"
if [ ! -d "/opt/spark" ]; then
    echo "Apache Spark not found. Downloading..."
    wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
    sudo mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

    echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
    echo "export PATH=\$SPARK_HOME/bin:\$PATH" >> ~/.bashrc
    source ~/.bashrc
else
    echo "Apache Spark already installed. Skipping download."
fi

#check if sedona is already installed
if [ ! -d "/home/tim/sedona" ]; then
    echo "Sedona not found. Downloading..."
    echo "📁 Cloning Sedona benchmark repo..."
    git clone https://github.com/apache/sedona.git
    cd sedona

    git checkout master

    echo "🏗️ Building Sedona from source..."
    mvn clean install -DskipTests

    echo "✅ Sedona and Spark are ready."
else 
    echo "Sedona already installed. Skipping download and build."
fi 
EOF

echo "Configuring Spark master..."
ssh "$SSH_USER@$IP" << EOF
#!/bin/bash

set -e

# Explicitly define SPARK_HOME
export SPARK_HOME="/opt/spark"

echo "📁 Creating spark-env.sh..."
cp ${SPARK_HOME}/conf/spark-env.sh.template ${SPARK_HOME}/conf/spark-env.sh
chmod +x \$SPARK_HOME/conf/spark-env.sh

cat << EOL > ${SPARK_HOME}/conf/spark-env.sh
export SPARK_MASTER_HOST="${IP}"
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
EOL

echo "✅ spark-env.sh configured for public IP: ${IP}"

# Restart Spark to apply the new configuration
${SPARK_HOME}/sbin/stop-master.sh || true
${SPARK_HOME}/sbin/start-master.sh
EOF

#test run
ssh "$SSH_USER@$IP" << 'EOF'
#!/bin/bash
echo "Running Spark shell with Sedona..."
set -e
export SPARK_HOME="/opt/spark"
$SPARK_HOME/bin/spark-shell --jars ~/sedona/spark-shaded/target/sedona-spark-shaded-3.3_2.12-1.8.0-SNAPSHOT.jar
EOF