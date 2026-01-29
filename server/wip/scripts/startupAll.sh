export SSH_USER="tim"
export GCP_IP=$1
#check $1 for value, if set to aviation, insert aviation data

# scp ../experiment/read/aviation/config/* $SSH_USER@$GCP_IP:~/config/aviation/
#scp ../../data/processed/aviation/part* $SSH_USER@$GCP_IP:~/data/aviation/
#scp ../../data/processed/aviation/aviation_data*.zip $SSH_USER@$GCP_IP:~/data/aviation/
#scp ../../data/processed/aviation/trip_aviation_data*.zip $SSH_USER@$GCP_IP:~/data/aviation/
#scp scripts/data/insertAviationData.sh $SSH_USER@$GCP_IP:~/ 
#ssh $SSH_USER@$GCP_IP 'chmod +x ~/insertAviationData.sh; ~/insertAviationData.sh' 

#check $1 for value, if set to cycling, insert cycling data
#scp ../experiment/read/cycling/config/* $SSH_USER@$GCP_IP:~/config/cycling/
# scp ../../data/processed/cycling/part* $SSH_USER@$GCP_IP:~/data/cycling/
# scp ../../data/processed/cycling/traj_* $SSH_USER@$GCP_IP:~/data/cycling/
# scp ../../data/processed/cycling/trip_* $SSH_USER@$GCP_IP:~/data/cycling/
#scp scripts/data/insertCyclingData.sh $SSH_USER@$GCP_IP:~/ 
#ssh $SSH_USER@$GCP_IP 'chmod +x ~/insertCyclingData.sh; ~/insertCyclingData.sh' 
#same as above for raster and sensor data

#scp ../experiment/read/ais/config/* $SSH_USER@$GCP_IP:~/config/ais/
scp ../../data/processed/ais/part* $SSH_USER@$GCP_IP:~/data/ais/
scp ../../data/processed/ais/traj* $SSH_USER@$GCP_IP:~/data/ais/
scp ../../data/processed/ais/trip* $SSH_USER@$GCP_IP:~/data/ais/
#scp scripts/data/insertAISData.sh $SSH_USER@$GCP_IP:~/
#ssh $SSH_USER@$GCP_IP 'chmod +x ~/insertAISData.sh; ~/insertAISData.sh'

#scp ../experiment/read/movebank/config/* $SSH_USER@$GCP_IP:~/config/movebank/
#scp ../../data/processed/movebank/part* $SSH_USER@$GCP_IP:~/data/movebank/
#scp ../../data/processed/movebank/movebank_segments* $SSH_USER@$GCP_IP:/~/data/movebank/
#scp ../../data/processed/movebank/movebank_traj* $SSH_USER@$GCP_IP:/~/data/movebank/
#scp scripts/data/insertMoveBankData.sh $SSH_USER@$GCP_IP:~/
#ssh $SSH_USER@$GCP_IP 'chmod +x ~/insertMoveBankData.sh; ~/insertMoveBankData.sh'

#scp ../experiment/read/taxi/config/* $SSH_USER@$GCP_IP:~/config/taxi/
#scp ../../data/processed/taxi/part* $SSH_USER@$GCP_IP:~/data/taxi/
#scp ../../data/processed/taxi/taxi_segments* $SSH_USER@$GCP_IP:/~/data/taxi/
#scp ../../data/processed/taxi/taxi_traj* $SSH_USER@$GCP_IP:/~/data/taxi/
#scp scripts/data/insertTaxiData.sh $SSH_USER@$GCP_IP:~/
#ssh $SSH_USER@$GCP_IP 'chmod +x ~/insertTaxiData.sh; ~/insertTaxiData.sh'

#scp ../experiment/read/pedestrian/config/* $SSH_USER@$GCP_IP:~/config/pedestrian/
#scp ../../data/processed/pedestrian/part* $SSH_USER@$GCP_IP:~/data/pedestrian/
#scp ../../data/processed/pedestrian/pedestrian_segments* $SSH_USER@$GCP_IP:/~/data/pedestrian/
#scp ../../data/processed/pedestrian/pedestrian_traj* $SSH_USER@$GCP_IP:/~/data/pedestrian/
#scp scripts/data/insertPedestrianData.sh $SSH_USER@$GCP_IP:~/
#ssh $SSH_USER@$GCP_IP 'chmod +x ~/insertPedestrianData.sh; ~/insertPedestrianData.sh'