# Flight Data Generation
This script requires access to flight data from the Deutsche Flugsicherung. Please contact them in order to receive a sample dataset that then can be used for further data generation.  
The Python script requires the setup of a venv

```
 python3 -m venv venv       
 source venv/bin/activate 
 pip install --upgrade pip
 pip install -r requirements.txt

```
You then need to train the model on the data provided by the DFS, which requires the data to be in a specific format, that being
Flight_ID, airplane, origin, destination, Point (WKT Format), Timestamp, Altitude.  
Example:  
```
69187756;B738;LLBG;EHAM;POINT(8.739094938110629 51.17534355710495);2023-01-02 18:17:18;11581.7
```
You can then create a model based on the data as such
```
cd process_data 
python process_data.py <link/to/flight.csv>
python preprocess_to_tensor.py
```
Then you can generate trajectories using parallel workers as such

 ```
 python generate_parallele_traj.py --workers <number of workers>
 ```
 Example:
  ```
 python generate_parallele_traj.py --workers 4
 ```
