# Setting up MobilityDB and Citus together
Make sure to adjust the `variables.tf`file to match your configuration. Specifically, change the project and ssh key.
```
terraform init
terraform apply --auto-approve
```
## Set needed variables to connect to the machine
```
export SSH_USER=$(terraform output -raw ssh_user)
export GCP_IP=$(terraform output -raw external_ip_sut_manager)
```
## Copy files for tests onto the machine
Ensure that you have run the data preparation script found in the `data` folder.
### Copy all files (Warning: Will take a while)
scp ../../../data/merged\* $SSH_USER@$GCP_IP:/tmp/
scp ../../../data/trips\* $SSH_USER@$GCP_IP:/tmp/
### Copy a sample set for smaller tests
scp ../../../data/merged00.csv $SSH_USER@$GCP_IP:/tmp/
scp ../../../data/merged01.csv $SSH_USER@$GCP_IP:/tmp/
scp ../../../data/merged02.csv $SSH_USER@$GCP_IP:/tmp/
scp ../../../data/trips_merged00.csv $SSH_USER@$GCP_IP:/tmp/
scp ../../../data/trips_merged01.csv $SSH_USER@$GCP_IP:/tmp/
scp ../../../data/trips_merged02.csv $SSH_USER@$GCP_IP:/tmp/

## Connect to the manager and configure Citus to work properly
```
ssh $SSH_USER@$GCP_IP

sudo service postgresql restart
sudo -i -u postgres psql -c "SELECT citus_set_coordinator_host('mobdb-manager', 5432);"
sudo -i -u postgres psql -c "SELECT * FROM citus_add_node('mobdb-worker-0',5432);"
sudo -i -u postgres psql -c "SELECT * FROM citus_add_node('mobdb-worker-1',5432);"
sudo -i -u postgres psql -c "SELECT * FROM citus_add_node('mobdb-worker-2',5432);"

sudo -i -u postgres psql


sudo -i -u postgres psql -c "SET citus.max_intermediate_result_size to -1;"
sudo -i -u postgres psql -c "CREATE TABLE cycling_data (
    ride_id float,
    rider_id float,
    latitude float,
    longitude float,
    x float,
    y float,
    z float,
    timestamp timestamp
);"

sudo -i -u postgres psql -c "SELECT create_distributed_table('cycling_data','rider_id');"


```
## Copy data into table and adjust table to include point data
```
sudo -i -u postgres psql -c "COPY cycling_data FROM '/tmp/merged00.csv' DELIMITER ',' CSV;"
sudo -i -u postgres psql -c "COPY cycling_data FROM '/tmp/merged01.csv' DELIMITER ',' CSV;"
sudo -i -u postgres psql -c "COPY cycling_data FROM '/tmp/merged02.csv' DELIMITER ',' CSV;"
sudo -i -u postgres psql -c "COPY cycling_data FROM '/tmp/merged03.csv' DELIMITER ',' CSV;"
sudo -i -u postgres psql -c "COPY cycling_data FROM '/tmp/merged04.csv' DELIMITER ',' CSV;"
sudo -i -u postgres psql -c "COPY cycling_data FROM '/tmp/merged05.csv' DELIMITER ',' CSV;"
sudo -i -u postgres psql -c "COPY cycling_data FROM '/tmp/merged06.csv' DELIMITER ',' CSV;"
sudo -i -u postgres psql -c "COPY cycling_data FROM '/tmp/merged07.csv' DELIMITER ',' CSV;"

sudo -i -u postgres psql -c "ALTER TABLE cycling_data ADD COLUMN point_geom geography(Point, 4326);"

-- Populate the new column with point geometries
sudo -i -u postgres psql -c "UPDATE cycling_data SET point_geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);"

sudo -i -u postgres psql -c "CREATE INDEX idx_cycling_data_point_geom ON cycling_data USING GIST (point_geom);"
sudo -i -u postgres psql -c "CREATE INDEX idx_cycling_data_latitude ON cycling_data (latitude);"
sudo -i -u postgres psql -c "CREATE INDEX idx_cycling_data_longitude ON cycling_data (longitude);"

```

## Create trip data as a comparison in a separate table
```
sudo -i -u postgres psql -c "CREATE TABLE cycling_trips (
  ride_id float,
  rider_id float,
  trip tgeogpoint
);"
sudo -i -u postgres psql -c "SELECT create_distributed_table('cycling_trips','ride_id');"
sudo -i -u postgres psql -c "COPY cycling_trips FROM '/tmp/trips_merged00.csv' DELIMITER ';' CSV HEADER;"
sudo -i -u postgres psql -c "COPY cycling_trips FROM '/tmp/trips_merged01.csv' DELIMITER ';' CSV HEADER;"
sudo -i -u postgres psql -c "COPY cycling_trips FROM '/tmp/trips_merged02.csv' DELIMITER ';' CSV HEADER;"
sudo -i -u postgres psql -c "COPY cycling_trips FROM '/tmp/trips_merged03.csv' DELIMITER ';' CSV HEADER;"
sudo -i -u postgres psql -c "COPY cycling_trips FROM '/tmp/trips_merged04.csv' DELIMITER ';' CSV HEADER;"
sudo -i -u postgres psql -c "COPY cycling_trips FROM '/tmp/trips_merged05.csv' DELIMITER ';' CSV HEADER;"
sudo -i -u postgres psql -c "COPY cycling_trips FROM '/tmp/trips_merged06.csv' DELIMITER ';' CSV HEADER;"
sudo -i -u postgres psql -c "COPY cycling_trips FROM '/tmp/trips_merged07.csv' DELIMITER ';' CSV HEADER;"
```


## Anything involving complex joins requires a reference table
A reference table is not distributed, but rather replicated across multiple worker nodes to enable complex joins. This should optimally only be used for small data samples, as the replication obviously requires additional space when compared to a distributed table. 
```
sudo -i -u postgres psql -c "CREATE TABLE cycling_trips_ref (
    ride_id float PRIMARY KEY,
    rider_id float,
    trip tgeogpoint 
);"

sudo -i -u postgres psql -c "SELECT create_reference_table('cycling_trips_ref');"
sudo -i -u postgres psql -c "INSERT INTO cycling_trips_ref SELECT * FROM cycling_trips;"
``` 
Check if table is filled:
```
sudo -i -u postgres psql -c "SELECT ride_id, rider_id FROM cycling_trips_ref ORDER BY ride_id LIMIT 50;"
```
### Sample Complex Join Query 
This join evaluates intersections of trips across the reference table.
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
  cycling_trips_ref a
JOIN 
  cycling_trips_ref b 
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
GCP_IP2=$(terraform output -raw external_ip_client)
SSH_USER2=$(terraform output -raw ssh_user)
#copy the benchmark code to the benchmark machine
scp -r ../../benchmark/mobilitydb/* $SSH_USER2@$GCP_IP2:/tmp/
ssh $SSH_USER2@$GCP_IP2

