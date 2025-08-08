#!/bin/bash
set -e
USER="tim"  # Change this if needed
IP="${1:-141.23.28.216}"  # Default IP if not provided
echo $IP
#aviation
scp -P 60002 ../processed/aviation/point*.csv $USER@$IP:/home/tim/data/aviation/
#testwise only copy one
#scp ../processed/aviation/point_NRW_HIGH_01*.csv $USER@$IP:/home/tim/data/aviation/
#resources 
scp -P 60002 ../processed/aviation/resources/* $USER@$IP:/home/tim/data/aviation/resources/
#insert the data into tables

scp -P 60002 ../processed/ais/point*.csv $USER@$IP:/home/tim/data/ais/
