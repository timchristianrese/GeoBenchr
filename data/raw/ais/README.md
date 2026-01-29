## The Piraeus AIS Dataset for Large-scale Maritime Data Analytics (AIS Dynamic Data)
---

  * **Object**: AIS-related data
  * **File name**: unipi_ais_dynamic_\<MONTH\>2019.csv
  * **Source**: Univ. Piraeus
  * **Dataset version**: ver. 2020-10-02
  * **SRID**: WGS84 (EPSG:4326)
  * **Coverage**: Saronic Gulf, Greece
  * **Volume**: 92,534,303 records (6.2 GB)
  * **Licence**: Attribution-NonCommercial-ShareAlike 4.0 International (CC BY-NC-SA 4.0)
  * **Description**: 12 CSV flat files containing AIS kinematic information

### Data Description
---

| Attribute   	| Data type 	| Description                                                                                                     	|
|-------------	|-----------	|-----------------------------------------------------------------------------------------------------------------	|
| t(imestamp) 	| Long      	| UNIX timestamp - number of milliseconds since **01/01/1970*                                                     	|
| vessel_id    	| integer   	| The identifier of the vessel											                                          	|
| lon         	| double    	| Longitude of transmitted vessel location                                                                        	|
| lat         	| double    	| Latitude of transmitted vessel location                                                                         	|
| heading     	| double    	| True heading of the vessel (i.e. the direction in which the vessel is pointing)                                 	|
| course      	| double    	| Course over Ground (CoG) of the vessel (i.e. the actual direction of motion - the intended direction of travel) 	|
| speed       	| double    	| The vessel's speed                                                                                              	|
