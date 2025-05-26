# GeoBenchr
Welcome to Geobenchr, an application benchmark tool for single-node and distributed spatiotemporal databases.
Geobenchr currently supports both MobilityDB and GeoMesa Accumulo and other platforms are currently being worked on. 
## Getting started 
Specific documentation for setting up MobilityDB and GeoMesa can be found in their respective folders. 
However, general information for what Geobenchr aims to be can be found further below.
### GCloud authentication
Authenticate with the GCloud CLI by doing the following (GCloud CLI needs to be installed in order for this to work)
```
gcloud auth login
gcloud auth application-default login
```
You should now be authenticated with the Google Cloud. Make sure that you're in the correct project.
### Setting up Benchmark infrastructure
Please refer to the `readme` found in the respective folder for the system you want to setup for benchmark procedures.
### Included Datasets
As of now, Geobenchr relies on  data from SimRa, a cycling application. A script to convert data into a format that is usable for both databases can be found in the `data`folder.  Data can be gotten from the GitHub repository or the DepositOnce site behind it: https://github.com/simra-project/dataset?tab=readme-ov-file/ or as an example from DepositOnce: https://depositonce.tu-berlin.de/items/212ae8b8-7376-47c1-bae2-09103c622de2 
We also include aviation data, which is semi-publically available (pending a registration with the service). We plan on swapping to a more detailed dataset for our aviation application in the future, as flight data here is given with large gaps.
## Evaluated Databases
As of now, GeoBenchr supports the following database systems:
- GeoMesa-Accumulo
- MobilityDB
- PostGIS
## Evaluated Data Formats
Applications may save their data in varying formats, which is why GeoBenchr evaluates three different data structures across its benchmark:
- Point data  
Each given point is saved separately of one another with its location and timestamp, along with its other attributes.
- Trip data  
While this is not relevant for all spatiotemporal data, many use cases include a moving object that creates a trip. Collected points along the way are stored as a single row within the database, along with all locations and timestamps
- Segmented trip data  
Storing an entire trip may become a hassle and inefficient, especially when a trip includes a large amount of points. Therefore, an alternative is to save data as a segmented line, which in combination with the other segments form the entire trip. A point is saved with its next (i.e the next generated) point, along with start and end timestamps of that line. 
## Application Benchmark Queries 
Our benchmark aims to support various query types, as applications may have varying requests to a database depending on the kind of application. The queries below are in our opionion commonly implemented in applications relying on spatiotemporal data
1. Spatial Queries
2. Temporal Queries
3. Spatiotemporal Queries
