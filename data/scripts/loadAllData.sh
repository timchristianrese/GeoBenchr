#!/bin/bash
size=$1
# Load all data required for benchmarks
nohup ./loadData.sh all ais $1 
nohup ./loadData.sh all aviation $1
nohup ./loadData.sh all cycling $1

