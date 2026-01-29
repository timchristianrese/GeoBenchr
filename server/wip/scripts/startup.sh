export SSH_USER="tim"
export GCP_IP=$2
#check $1 for value, if set to aviation, insert aviation data
if [ "$1" == "aviation" ]; then
    # scp ../experiment/read/aviation/config/* $SSH_USER@$GCP_IP:~/config/aviation/
    #scp ../../data/processed/aviation/parts* $SSH_USER@$GCP_IP:~/data/aviation/
    #scp ../../data/processed/aviation/aviation_data*.zip $SSH_USER@$GCP_IP:~/data/aviation/
    #scp ../../data/processed/aviation/trip_aviation_data*.zip $SSH_USER@$GCP_IP:~/data/aviation/
    #scp scripts/data/insertAviationData.sh $SSH_USER@$GCP_IP:~/ 
    #ssh $SSH_USER@$GCP_IP 'chmod +x ~/insertAviationData.sh; ~/insertAviationData.sh' 
fi
#check $1 for value, if set to cycling, insert cycling data
if [ "$1" == "cycling" ]; then
    #scp ../experiment/read/cycling/config/* $SSH_USER@$GCP_IP:~/config/cycling/
    scp ../../data/processed/cycling/parts* $SSH_USER@$GCP_IP:~/data/cycling/
    scp ../../data/processed/cycling/traj_* $SSH_USER@$GCP_IP:/~/data/cycling/
    scp ../../data/processed/cycling/trip_* $SSH_USER@$GCP_IP:~/data/cycling/
    #scp scripts/data/insertCyclingData.sh $SSH_USER@$GCP_IP:~/ 
    #ssh $SSH_USER@$GCP_IP 'chmod +x ~/insertCyclingData.sh; ~/insertCyclingData.sh' 
fi
#same as above for raster and sensor data

#ais
if [ "$1" == "ais" ]; then
    #scp ../experiment/read/ais/config/* $SSH_USER@$GCP_IP:~/config/ais/
    scp ../../data/processed/ais/parts* $SSH_USER@$GCP_IP:~/data/ais/
    scp ../../data/processed/ais/ais_segments* $SSH_USER@$GCP_IP:/~/data/ais/
    scp ../../data/processed/ais/ais_traj* $SSH_USER@$GCP_IP:/~/data/ais/
    #scp scripts/data/insertAISData.sh $SSH_USER@$GCP_IP:~/
    #ssh $SSH_USER@$GCP_IP 'chmod +x ~/insertAISData.sh; ~/insertAISData.sh'
fi

#movebank
if [ "$1" == "movebank" ]; then
    #scp ../experiment/read/movebank/config/* $SSH_USER@$GCP_IP:~/config/movebank/
    scp ../../data/processed/movebank/parts* $SSH_USER@$GCP_IP:~/data/movebank/
    scp ../../data/processed/movebank/movebank_segments* $SSH_USER@$GCP_IP:/~/data/movebank/
    scp ../../data/processed/movebank/movebank_traj* $SSH_USER@$GCP_IP:/~/data/movebank/
    #scp scripts/data/insertMoveBankData.sh $SSH_USER@$GCP_IP:~/
    #ssh $SSH_USER@$GCP_IP 'chmod +x ~/insertMoveBankData.sh; ~/insertMoveBankData.sh'
fi
#taxi
if [ "$1" == "taxi" ]; then
    #scp ../experiment/read/taxi/config/* $SSH_USER@$GCP_IP:~/config/taxi/
    scp ../../data/processed/taxi/parts* $SSH_USER@$GCP_IP:~/data/taxi/
    scp ../../data/processed/taxi/taxi_segments* $SSH_USER@$GCP_IP:/~/data/taxi/
    scp ../../data/processed/taxi/taxi_traj* $SSH_USER@$GCP_IP:/~/data/taxi/
    #scp scripts/data/insertTaxiData.sh $SSH_USER@$GCP_IP:~/
    #ssh $SSH_USER@$GCP_IP 'chmod +x ~/insertTaxiData.sh; ~/insertTaxiData.sh'
fi
#pedestrian
if [ "$1" == "pedestrian" ]; then
    #scp ../experiment/read/pedestrian/config/* $SSH_USER@$GCP_IP:~/config/pedestrian/
    scp ../../data/processed/pedestrian/parts* $SSH_USER@$GCP_IP:~/data/pedestrian/
    scp ../../data/processed/pedestrian/pedestrian_segments* $SSH_USER@$GCP_IP:/~/data/pedestrian/
    scp ../../data/processed/pedestrian/pedestrian_traj* $SSH_USER@$GCP_IP:/~/data/pedestrian/
    #scp scripts/data/insertPedestrianData.sh $SSH_USER@$GCP_IP:~/
    #ssh $SSH_USER@$GCP_IP 'chmod +x ~/insertPedestrianData.sh; ~/insertPedestrianData.sh'
fi