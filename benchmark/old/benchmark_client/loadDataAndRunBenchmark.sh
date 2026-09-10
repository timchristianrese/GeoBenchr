scenario=$1
nohup ../../data/loadData.sh mobilitydb $scenario 1B
nohup ./runBenchmark.sh $scenario


