import threading
import random
import time
import os
import csv
import sys
import subprocess
import json
import yaml


def generate_random_position_in_Berlin():
    poslong = random.uniform(13.088346, 13.761160)
    poslat = random.uniform(52.338049, 52.675454)
    return poslong, poslat

def clear_table(table):
    try:
        
        print(f"Table {table} cleared successfully")
    except (Exception) as error:
        print("Error while connecting", error)
    finally:
        #wrap up benchmark
        print("Done")

def initial_insert():
    
    #empty command for now
    pass
    # try:

    # except:

    # try:

    # except:


def get_max_ride_id():
    try:
        #get max ride ID from GeoMesa

        # cursor = 
        # cursor.execute("SELECT MAX(ride_id) FROM cycling_data;")
        records = ""
        return records[0][0]
    except (Exception) as error:
        print("Error while connecting", error)
    finally:
        print("Done")
def get_terraform_output(deployment = "multi"):
    original_dir = os.getcwd()

    path = os.path.join("../../../geomesa-accumulo/gcp", deployment)

    # Change the current working directory
    os.chdir(path)

    # Run the terraform output command
    result = subprocess.run(['terraform', 'output', '-json'], stdout=subprocess.PIPE)
    print(result)
    # Parse the output as JSON
    output = json.loads(result.stdout)
    # ... change the directory and do stuff ...
    os.chdir(original_dir)
    return output


def execute_query(query_type, limit):
    try:
    #match case in python
        match query_type:
            case "surrounding":
                poslong, poslat = generate_random_position_in_Berlin()
                #replace -q "" with -q"$query"
                query = f"DWITHIN(geom, POINT({poslong} {poslat}), 5000, meters)"
                final_query = ssh_point.replace("-q \"\"", f"-q \"{query}\"")
                if(limit == -1):
                    final_query = final_query.replace("-m", "")
                else:
                    final_query = final_query.replace("-m", f"-m {limit}")
                print(final_query)
                
                start = time.time()
                #run the query in the shell
                result = subprocess.run(final_query, shell=True, stdout=subprocess.PIPE)

                #run the query here
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = ""

                print(records)
                
                # get paths between two random points

            #get intersections of a specific ride    
            case "ride_traffic":
                ride_id = random.randint(1, 596)   
                query = f"INTERSECTS(geom,SRID=4326;(SELECT UNION(geom) FROM rides WHERE ride_id = {ride_id})) AND ride_id != {ride_id}"
                final_query = ssh_point.replace("-q \"\"", f"-q \"{query}\"")
                if(limit == -1):
                    final_query = final_query.replace("-m", "")
                else:
                    final_query = final_query.replace("-m", f"-m {limit}")
                print(final_query)
                start = time.time()
                #run the query in the shell
                result = subprocess.run(final_query, shell=True, stdout=subprocess.PIPE)

                #run the query here
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = ""

                print(records)
            #requires a complex join, therefore needs a reference table to join, setup is found in readme.md of multi
            case "intersections":
               
                poslongstart, poslatstart = generate_random_position_in_Berlin()
                poslongend, poslatend = generate_random_position_in_Berlin()
                
                query ="WHERE INTERSECTS(geom, LINESTRING({poslongstart} {poslatstart}, {poslongend} {poslatend}))"
                final_query = ssh_point.replace("-q \"\"", f"-q \"{query}\"")
                if(limit == -1):
                    final_query = final_query.replace("-m", "")
                else:
                    final_query = final_query.replace("-m", f"-m {limit}")
                print(final_query)
                
                start = time.time()
                #run the query in the shell
                result = subprocess.run(final_query, shell=True, stdout=subprocess.PIPE)

                #run the query here
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = ""

                print(records)
            #insert a single trip into the database
            case "insert_ride":
                path_length = 0
                
                random_rider_id = random.randint(1, 30)
                ride_id = get_max_ride_id() + 1
                random_latitude = random.uniform(52.338049, 52.675454)
                random_longitude = random.uniform(13.088346, 13.761160)
                ride_date = time.strftime('%Y-%m-%d %H:%M:%S')
                initial_start = time.strftime('%Y-%m-%d %H:%M:%S.%f')[:3]
                duration = 0
                while path_length < limit:
                    ride_date = time.strftime('%Y-%m-%d %H:%M:%S.%f')[:3]
                    
                    random_latitude  += random.uniform(-0.000001, 0.000001)
                    random_longitude += random.uniform(-0.000001, 0.000001)
                    ride_date = time.strftime('%Y-%m-%d %H:%M:%S.%f')[:3]

                    start = time.time()
                  # get time after executing query
                    end = time.time()
                    duration += end - start
                    path_length += 1
                end = time.strftime('%Y-%m-%d %H:%M:%S.%f')[:3]
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},1,{initial_start},{end},{duration}\n")
                print(f"Ride {ride_id} of length {path_length} inserted successfully into cycling_data table")
                
            #bulk insert ride_data

            case "bulk_insert_rides":
                rides_inserted = 0
                ride_id = get_max_ride_id() + 1
                while rides_inserted < limit:
                    path_length = 0
                    random_rider_id = random.randint(1, 30)
                    
                    random_latitude = random.uniform(52.338049, 52.675454)
                    random_longitude = random.uniform(13.088346, 13.761160)
                    ride_date = time.strftime('%Y-%m-%d %H:%M:%S')
                    initial_start = time.strftime('%Y-%m-%d %H:%M:%S.%f')[:3]
                    duration = 0
                    while path_length < limit:
                        ride_date = time.strftime('%Y-%m-%d %H:%M:%S.%f')[:3]
                        random_latitude  += random.uniform(-0.000001, 0.000001)
                        random_longitude += random.uniform(-0.000001, 0.000001)
                        ride_date = time.strftime('%Y-%m-%d %H:%M:%S.%f')[:3]
                        
                        start = time.time()
                        
                        end = time.time()
                        duration += end - start
                        path_length += 1
                    end = time.strftime('%Y-%m-%d %H:%M:%S.%f')[:3]
                    rides_inserted += 1
                    ride_id += 1
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},1,{initial_start},{end},{duration}\n")
                print(f"Ride {ride_id} of length {path_length} inserted successfully into cycling_data table")

            case "bounding_box":
                poslong, poslat = generate_random_position_in_Berlin()
                
                print(query)
                start = time.time()
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = ""
                print(records)

            case "polygonal_area":
                # for now, static polygonal area
                lat1 , lon1 = generate_random_position_in_Berlin()
                lat2 , lon2 = generate_random_position_in_Berlin()
                lat3 , lon3 = generate_random_position_in_Berlin()
                lat4 , lon4 = generate_random_position_in_Berlin()
                
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = ""
                print(records)
            
            #temporal query POSTGIS style
            case "time_interval":

                # define start and end time for the query
                start_time = "2022-07-01 00:00:00"
                end_time = "2023-07-01 01:00:00"

                start = time.time()
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = ""
                print(records)

            #MobilityDB feature test
            case "get_trip":
                # define start and end time for the query
                ride_id = random.randint(1, 400)
               
                start = time.time()
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                print(records)
            case "get_trip_length":
                # define start and end time for the query
                ride_id = random.randint(1, 400)
                
                print(query)
                start = time.time()
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = ""
                print(records)
            #MobiilityDB temporal support test
            case "get_trip_duration":
                # define start and end time for the query
                ride_id = random.randint(1, 400)
               
                start = time.time()
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = ""
                print(records)
            case "get_trip_speed":
                # define start and end time for the query
                ride_id = random.randint(1, 400)
                
                start = time.time()
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = ""
                print(records)
            case "interval_around_timestamp":
                # define start and end time for the query
                start_time = "2023-07-01 00:00:00"

                start = time.time()
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = ""
                print(records)
            case "spatiotemporal":
                # define start and end time for the query
                start_time = "2023-07-01 00:00:00"
                end_time = "2023-07-01 01:00:00"
                poslong, poslat = generate_random_position_in_Berlin()
                start = time.time()
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = ""
                print(records)

    except (Exception) as error:
        print("Error while connecting", error)

    finally:
        print("Done")
# List of queries to execute
def run_threads(num_threads, query_type, limit):
    threads = []

    
    for i in range(num_threads):
        thread = threading.Thread(target=execute_query, args=(query_type, limit))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
        # clear the threads list
        threads.clear()

# Create and start a thread until the number of threads is reached
# run mini benchmark
#setup benchmark for geomesa instead of mobilityDB, but use same queries in CQL
#set deployment variable if you want to use single or multi node setup


if len(sys.argv) < 2:
    print("Please provide the deployment type (single/multi) as an argument")
    sys.exit(1)
deployment = sys.argv[1]
if(deployment == ""):
    deployment = "multi"
terraform_output = get_terraform_output(deployment)
#get variables ssh_user and ip from terraform output
ssh_user = terraform_output["ssh_user"]["value"]
ip = ""
if(deployment == "single"):
    ip = terraform_output["external_ip_sut_manager"]["value"]
else:
    ip = terraform_output["external_ip_sut_namenode_manager"]["value"]

ssh_point = f"ssh {ssh_user}@{ip} '/opt/geomesa-accumulo/bin/geomesa-accumulo export -i test -z localhost -u root  -p test -c example -m -q \"\" -f ride_data'"
ssh_trip = "ssh $SSH_USER@$GCP_IP '/opt/geomesa-accumulo/bin/geomesa-accumulo export -i test -z localhost -u root  -p test -c example -m -q \"\" -f trip_data'"



#initial insert of data if not done on machine, 
#clear_table('cycling_data')
#clear_table('cycling_trips')
#initial_insert()



#Configure the benchmark
#run_threads(#Number of parallel threads, default query to use, query type, limit)


#TODO fix this and possibly use yaml for configuration
# benchmark_config = yaml.safe_load(open("benchmark_conf.yaml"))
# #print items of surrounding from benchmark_config
# print(benchmark_config['surrounding'])

# surrounding = benchmark_config['surrounding']
# ride_traffic = benchmark_config['ride_traffic']
# intersections = benchmark_config['intersections']
# insert_ride = benchmark_config['insert_ride']
# bulk_insert_rides = benchmark_config['bulk_insert_rides']
# bounding_box = benchmark_config['bounding_box']
# polygonal_area = benchmark_config['polygonal_area']
# time_interval = benchmark_config['time_interval']
# get_trip = benchmark_config['get_trip']
# get_trip_length = benchmark_config['get_trip_length']
# get_trip_duration = benchmark_config['get_trip_duration']
# get_trip_speed = benchmark_config['get_trip_speed']
# interval_around_timestamp = benchmark_config['interval_around_timestamp']
# spatiotemporal = benchmark_config['spatiotemporal']
#run threads with the configurations instead of hardcoding


#Run the benchmark
#run_threads(1, "surrounding", -1)
run_threads(1, "ride_traffic", -1)
#run_threads(2, default_query, "ride_traffic", 50)
#run_threads(2, default_query, "intersections", 50)
#run_threads(2, default_query, "insert_ride", 10)
#run_threads(1, default_query, "bulk_insert_rides", 10)
#run_threads(2, default_query, "bounding_box", 50)
#run_threads(2, default_query, "polygonal_area", 50)
#run_threads(2, default_query, "time_interval", 50)
#run_threads(2, default_query, "get_trip", 50)
#run_threads(2, default_query, "get_trip_length", 50)
#run_threads(2, default_query, "get_trip_duration", 50)
#run_threads(2, default_query, "get_trip_speed", 50)
#run_threads(2, default_query, "interval_around_timestamp", 50)
#run_threads(2, default_query, "spatiotemporal", 50)
