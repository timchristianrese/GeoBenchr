sudo apt-get update
sudo apt install postgresql-client-common -y
sudo apt-get install postgresql-client -y
sudo apt-get install ca-certificates curl -y
sudo install -m 0755 -d /etc/apt/keyrings -y
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

sudo apt-get install docker-ce docker-compose docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y

git clone https://github.com/MobilityDB/MobilityDB-docker
cd MobilityDB-docker/14-3.2-1
docker-compose up -d
export PGPASSWORD="docker"
#psql -h localhost -p 25432 -d mobilitydb -U docker