
# Cycling Data Generation
The Python script requires the setup of a venv

```
 python3 -m venv venv       
 source venv/bin/activate 
 pip install --upgrade pip
 pip install -r requirements.txt
 cd graph  
 python build_graph.py
```

We also need to run all preprocessing steps for the data:

```
cd ../preprocess
python convert_to_json.py  
python convert_all_gps_to_nodes.py
python prepare_training_dataset.py
```

Afterwards, we train the model on the data:
```
cd ..
python train.py
``` 

Then you can generate trajectories using parallel workers as such

 ```
python generate_noisy_time_traj_new.py <num_workers>
 ```
Example:
  ```
 python generate_noisy_time_traj_new.py 4
 ```
(`generate_noisy_time_traj.py`, the older version this replaced, has moved to `old/` -- its output
directory is `output/parquet_old/`, the current script writes to `output/parquet/`.)
