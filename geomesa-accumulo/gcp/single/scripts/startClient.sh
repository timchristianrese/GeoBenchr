# Update package list
sudo apt update

# Install Java JDK 1.8
echo "Installing Java JDK 1.8..."
sudo apt install -y openjdk-8-jdk maven git </dev/null

cd ~/geotools/geotools
mvn install


