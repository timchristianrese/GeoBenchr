GCP_IP2=$(terraform output -raw external_ip_benchmark)
SSH_USER=$(terraform output -raw ssh_user)
scp -r ../../benchmark/geomesa/geotools/ $SSH_USER@$GCP_IP2:/tmp/
ssh $SSH_USER@$GCP_IP2

# Update package list
sudo apt update

# Install Java JDK 1.8
echo "Installing Java JDK 1.8..."
sudo apt install -y openjdk-8-jdk maven git </dev/null

cd /tmp/geotools/geotools
mvn install


#run the benchmark
mvn exec:java -Dexec.mainClass="com.example.Main"