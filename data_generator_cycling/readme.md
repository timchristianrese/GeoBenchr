
# Cycling Data Generation
The Python script requires the setup of a venv

```
 python3 -m venv venv       
 source venv/bin/activate 
 pip install --upgrade pip
 pip install -r requirements.txt
 cd graph  
 python buildGraph.py
```

Then you can generate trajectories using parallel workers as such

 ```
python generate_noisy_time_traj.py 4
 ```
 Example:
  ```
 python generate_noisy_time_traj.py 4
 ```
