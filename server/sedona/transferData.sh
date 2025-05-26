SSH_USER="tim"
IP="${1:-10.35.0.22}"
ssh $SSH_USER@$IP "mkdir -p /home/tim/data/aviation/; mkdir -p /home/tim/data/cycling; mkdir -p /home/tim/data/ais/; mkdir -p /home/tim/data/pedestrian/; mkdir -p /home/tim/data/movebank/; mkdir -p /home/tim/data/taxi/"

scp ../../data/processed/aviation/NRW_HIGH_01*.csv $SSH_USER@$IP:/home/tim/data/aviation/