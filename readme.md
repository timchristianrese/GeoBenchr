# GeoBenchr
Welcome to GeoBenchr, an application-centric benchmarking suite for spatiotemporal database platforms. In our current work, we evaluate several systems with three unique application scenarios based on real-world spatiotemporal data, and compare platform performance across these domains.
## Included Scenarios
As of now, GeoBenchr provides application scenarios for an aviation, cycling, and ais application. We provide several queries with which we evaluate system performance, and compare different data scales, scalability factors, configuration parameters, and cross-platform performance.
## Evaluated Databases
As of now, GeoBenchr supports the following database systems:
- SedonaDB
- PostGIS
- SpaceTime
- TimeScaleDB
- MobilityDB

## Setup 
The benchmark deployment is split into separate categories for clarity, those being `server`, `data`, and `client`.  All steps are necessary in order to run a benchmark experiment with GeoBenchr, in the above mentioned order. Detailed setup instructions for each category can be found in the subdirectories (in nearly all cases, in the `single` subfolder)
## Results
Some of the results are included in the paper. All generated graphs are hosted here in the repository, found [here] (./analysis/results)


