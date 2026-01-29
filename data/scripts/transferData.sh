#!/bin/bash
set -e
USER=$2  # Change this if needed
IP="${1:-141.23.28.216}"  # Default IP if not provided
echo $IP
#create the necessary folders, should they not exist
ssh -p 60002 $USER@$IP "mkdir -p /home/$USER/data/aviation/resources/"
ssh -p 60002 $USER@$IP "mkdir -p /home/$USER/data/ais/resources/"
ssh -p 60002 $USER@$IP "mkdir -p /home/$USER/data/cycling/resources/"

#aviation
scp -P 60002 ../processed/aviation/point*.csv $USER@$IP:/home/$USER/data/aviation/
#testwise only copy one
#scp ../processed/aviation/point_NRW_HIGH_01*.csv $USER@$IP:/home/$USER/data/aviation/
#resources 
scp -P 60002 ../processed/aviation/resources/* $USER@$IP:/home/$USER/data/aviation/resources/
#insert the data into tables

scp -P 60002 ../processed/ais/point*.csv $USER@$IP:/home/$USER/data/ais/
