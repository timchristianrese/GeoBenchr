# MobilityDB-Docker
We recommend using either the `single`or `multi` configuration in GCP, however if you require local testing, this is a valid alternative.
## Requirements
- Linux or a Mac system
- Docker installed
- Docker compose configured

## Getting started
Pull the docker image 
```
docker pull govner/mobdb:latest
````
Start the containers using `docker compose`
```
docker compose up -d
```
Transfer the startup scripts and sample data to the relevant containers
```
docker cp scripts/startWorker.sh docker-worker1-1:/startWorker.sh
docker cp scripts/startWorker.sh docker-worker2-1:/startWorker.sh
docker cp scripts/startWorker.sh docker-worker3-1:/startWorker.sh
docker cp scripts/startWorker.sh docker-worker4-1:/startWorker.sh
docker cp scripts/startWorker.sh docker-worker5-1:/startWorker.sh

docker cp scripts/startCoordinator.sh docker-coordinator-1:/startCoordinator.sh
docker cp data/sample_ride_data.csv docker-coordinator-1:/sample_ride_data.csv
docker cp data/sample_trip_data.csv docker-coordinator-1:/sample_trip_data.csv

## Run the startup scripts and ingest the sample data
```
docker exec docker-worker1-1 bin/sh -c "chmod +x startWorker.sh; ./startWorker.sh"
docker exec docker-worker2-1 bin/sh -c "chmod +x startWorker.sh; ./startWorker.sh"
docker exec docker-worker3-1 bin/sh -c "chmod +x startWorker.sh; ./startWorker.sh"
docker exec docker-worker4-1 bin/sh -c "chmod +x startWorker.sh; ./startWorker.sh"
docker exec docker-worker5-1 bin/sh -c "chmod +x startWorker.sh; ./startWorker.sh"

docker exec docker-coordinator-1 bin/sh -c "chmod +x startCoordinator.sh; ./startCoordinator.sh"
```
## Connect to the manager and setup data source

### Sample Complex Join Query 
#### Log in to the PSQL shell
```
sudo -i -u postgres psql
```
#### Run the sample query 
``` 
SELECT 
  a.ride_id AS trip_id_1, 
  b.ride_id AS trip_id_2, 
  a.trip && b.trip AS intersects
FROM 
  cycling_trips a
JOIN 
  cycling_trips b 
ON 
  a.ride_id <> b.ride_id  
WHERE 
  a.trip && b.trip;
```

## Local benchmark
Open another terminal window and navigate to the `benchmark/mobilitydb` folder. From the current folder:
```
cd ../../../benchmark/mobilitydb
```
Run the benchmark using: 
```
python3 runMiniBenchmark.py $GCP_IP 5432
```
## Benchmark client usage

### Load Benchmark onto client
```
GCP_IP2=$(terraform output -raw external_ip_client)
SSH_USER2=$(terraform output -raw ssh_user)
#copy the benchmark code to the benchmark machine
scp -r ../../benchmark/mobilitydb/* $SSH_USER2@$GCP_IP2:/tmp/
ssh $SSH_USER2@$GCP_IP2
```
