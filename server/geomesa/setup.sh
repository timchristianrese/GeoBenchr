export SSH_USER=$1
export IP=$2
scp scripts/startManager.sh $SSH_USER@$IP:~/ 
scp ../../../data/geomesa*.csv $SSH_USER@$IP:~/ 
ssh $SSH_USER@$IP 'chmod +x ~/startManager.sh; ~/startManager.sh'
ssh $SSH_USER@$IP "/opt/hadoop/bin/hdfs namenode -format"
ssh $SSH_USER@$IP "/opt/hadoop/sbin/start-dfs.sh"
ssh $SSH_USER@$IP "/opt/accumulo/bin/accumulo init --instance-name test -u root --password test"
ssh $SSH_USER@$IP "ulimit -n 32768;/opt/accumulo/bin/accumulo-cluster start"
ssh $SSH_USER@$IP 'cd /opt/geomesa-accumulo;yes | bin/install-dependencies.sh;yes | bin/install-shapefile-support.sh'
ssh $SSH_USER@$IP 'cd /opt/geomesa-accumulo; bin/geomesa-accumulo create-schema -i test -z localhost -u root  -p test -c example -s "ride_id:Integer:index=full,rider_id:Integer:index=full,latitude:Double,longitude:Double,geom:Point:srid=4326,x:Double,y:Double,z:Double,timestamp:Date" -f ride_data'
ssh $SSH_USER@$IP 'cd /opt/geomesa-accumulo; bin/geomesa-accumulo create-schema -i test -z localhost -u root  -p test -c example -s "ride_id:Integer:index=full,rider_id:Integer:index=full,trip:MultiLineString:srid=4326,timestamp:List[Date]" -f trip_data'
scp ../../converter/ride_data.converter $SSH_USER@$IP:/opt/geomesa-accumulo
scp ../../converter/trip_data.converter $SSH_USER@$IP:/opt/geomesa-accumulo
ssh $SSH_USER@$IP '/opt/geomesa-accumulo/bin/geomesa-accumulo ingest -C /opt/geomesa-accumulo/ride_data.converter -c example -i test -z localhost -u root -p test -f ride_data -t 8 ~/geomesa_merged*.csv'
ssh $SSH_USER@$IP '/opt/geomesa-accumulo/bin/geomesa-accumulo ingest -C /opt/geomesa-accumulo/trip_data.converter -c example -i test -z localhost -u root -p test -f trip_data -t 1 ~/geomesa_trips_geomesa_merged*.csv'
ssh $SSH_USER@$IP '/opt/geomesa-accumulo/bin/geomesa-accumulo export -i test -z localhost -u root  -p test -c example -m 50 -q "rider_id <= 30" -f ride_data'
ssh $SSH_USER@$IP '/opt/geomesa-accumulo/bin/geomesa-accumulo export -i test -z localhost -u root  -p test -c example -m 50 -q "rider_id <= 30" -f trip_data'