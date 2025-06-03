#!/bin/bash
set -e
USER="tim"  # Change this if needed
IP="${1:-130.149.253.149}"  # Default IP if not provided
echo $IP
ssh $USER@$IP "mkdir -p /home/tim/data/aviation/; mkdir -p /home/tim/data/cycling; mkdir -p /home/tim/data/ais/; mkdir -p /home/tim/data/pedestrian/; mkdir -p /home/tim/data/movebank/; mkdir -p /home/tim/data/taxi/;"

#aviation
#scp ../processed/aviation/point*.csv $USER@$IP:/home/tim/data/aviation/
#testwise only copy one
scp ../processed/aviation/point_NRW_HIGH_01*.csv $USER@$IP:/home/tim/data/aviation/
#resources 
mkdir -p ../processed/aviation/resources/
scp ../processed/aviation/resources/* $USER@$IP:/home/tim/data/aviation/resources/
