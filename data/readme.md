# Loading Data
This step assumes that you have already set up the database systems and they are currently running on the machine. 
- First, we need to load the required resources onto the server. For this, several steps need to be completed. 
- For each scenario, we provide a small subset of raw data in this repository. The actual used amount of data is higher, but impractical to store in a repository (due to the significant repository size increase and file size limits)
- For the aviation data, please contact the Deutsche Flugsicherung in order to obtain data available for research. 
- All SimRa data was taken from the [Simra Project](https://github.com/simra-project/dataset)
- All AIS data was taken from [The Piraeus AIS Dataset for Large-scale Maritime Data Analytics] (https://www.bisec.gr/dataset/the-piraeus-ais-dataset-for-large-scale-maritime-data-analytics)
- We need to preprocess the raw mobility data into formats understandable by the database systems. This also creates the necessary folders on the machine
```
./prepareAISdata.py
./prepareAviationData.py
./prepareCyclingData.py
```

- Transfer the supporting datasets to the hosting instance, and create necessary folder structures with  
```
cd scripts
./loadResourcesOnServer.sh <ip> <user>
./transferData.sh <ip> <user>
```
- Finally, we need to actually load the data into the database systems. `loadData` is the main script for this, and has several parameters, such as platform (currently, only "all" is supported), scenario, and dataset size.
```
Example Usage of loadData.sh:
./loadData.sh all <ais|aviation|cycling> <1M|10M|100M|1B>
./loadData.sh all ais 1B # this would load the AIS data into all database systems (except SedonaDB and SpaceTime), and limit the dataset size to 1B points at most. 
```
- To actually load data, we run 
```
scp loadData.sh <user>@<ip>:/home/<user>/data
scp loadAllData.sh <user>@<ip>:/home/<user>/data
```
- Then, SSH onto the machine as the intended user and 
```
cd data
./loadAllData.sh <1M|10M|100M|1B> #loads all scenarios
```
or just load the scenarios you would like to evaluate by running `loadData` for your intended specification. 
```
Example:
./loadData.sh all cycling 1M
``` 
## SedonaDB
This requires the data to be first loaded into PostGIS, from which then the data is taken into SedonaDB. The full procedure for SedonaDB, including Data loading and benchmark experiments, can be found in [the Sedona Notebook](../benchmark/sedonadb.ipynb). 