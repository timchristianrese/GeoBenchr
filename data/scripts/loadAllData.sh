#!/bin/bash
# Load all data required for benchmarks
nohup ./loadData.sh mobilitydb ais 100M
nohup ./loadData.sh mobilitydb aviation 100M
nohup ./loadData.sh mobilitydb cycling 100M

