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
        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="test",
            host=hostname,
            port=portnum
        )
        cursor = connection.cursor()
        cursor.execute(f"DELETE FROM {table};")
        connection.commit()
        print(f"Table {table} cleared successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def initial_insert():
    try:

        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="test",
            host=hostname,
            port=portnum
        )
        counter = 1;
        while counter <= 7:
            file_path = "../../data/"
            file = file_path + "merged0" + str(counter) + ".csv"
            with open(file, 'r') as f:
                reader = csv.reader(f)
                row_counter = 0
                duration = 0
                #set last row to first row 
                last_row = next(reader) # skip header row and get first row
                for row in reader:
                    cursor = connection.cursor()
                    query = f"INSERT INTO cycling_data(ride_id, rider_id, latitude, longitude, x, y, z, timestamp, point_geom, line_geom) VALUES ({row[0]},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]},'{row[7]}', ST_SetSRID(ST_MakePoint({row[2]},{row[3]}), 4326), ST_MakeLine(ST_SetSRID(ST_MakePoint({last_row[2]},{last_row[3]}), 4326), ST_SetSRID(ST_MakePoint({row[2]},{row[3]}), 4326)));"
                    last_row = row
                    cursor.execute(query)
                    connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    try:

        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="test",
            host=hostname,
            port=portnum
        )
        counter = 1;
        while counter <= 7:
            file_path = "../../data/"
            file = file_path + "trips" + str(counter) + ".csv"
            with open(file, 'r') as f:
                reader = csv.reader(f, delimiter=';');
                row_counter = 0
                duration = 0
                #set last row to first row 
                last_row = next(reader) # skip header row and get first row
                for row in reader:
                    cursor = connection.cursor()
                    query = f"INSERT INTO cycling_trips(ride_id, rider_id, trip) VALUES ({row[0]},{row[1]},{row[2]},{row[3]});"
                    last_row = row
                    cursor.execute(query)
                    connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def get_max_ride_id():
    try:
        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="test",
            host=hostname,
            port=portnum
        )
        cursor = connection.cursor()
        cursor.execute("SELECT MAX(ride_id) FROM cycling_data;")
        records = cursor.fetchall()
        return records[0][0]
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()

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


def execute_query(query, query_type, limit):
    try:
    #match case in python
        match query_type:
            case "surrounding":
                poslong, poslat = generate_random_position_in_Berlin()
                #replace -q "" with -q"$query"
                final_query = ssh_point.replace()
                print(query)
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = cursor.fetchall()

                print(records)
                
                # get paths between two random points

            #get intersections of a specific ride    
            case "ride_traffic":
                cursor = connection.cursor()
                ride_id = random.randint(1, 596)   
                query_addition =f"SELECT a.ride_id AS trip_id_1, b.ride_id AS trip_id_2, a.trip && b.trip AS intersects FROM  {query_table} a JOIN  {query_table} b ON a.ride_id <> b.ride_id WHERE a.ride_id = {ride_id} AND a.trip && b.trip LIMIT {limit};"
                #again, remove default select all
                query = query_addition
                print(query)
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = cursor.fetchall()
                print(records)
            #requires a complex join, therefore needs a reference table to join, setup is found in readme.md of multi
            case "intersections":
                cursor = connection.cursor()
                poslongstart, poslatstart = generate_random_position_in_Berlin()
                poslongend, poslatend = generate_random_position_in_Berlin()
                query_table = "cycling_trips_ref"
                if deployment == "single":
                    query_table = "cycling_trips"
                query_addition =f"SELECT a.ride_id AS trip_id_1, b.ride_id AS trip_id_2, a.trip && b.trip AS intersects FROM {query_table} a JOIN {query_table} b ON a.ride_id <> b.ride_id  WHERE a.trip && b.trip LIMIT {limit};"
                #exclude default select all
                query = query_addition
                print(query)
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = cursor.fetchall()
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
                    insert_query = f"INSERT INTO cycling_data (ride_id, rider_id, latitude, longitude, x, y, z, timestamp, point_geom, line_geom) VALUES ({ride_id}, {random_rider_id}, {random_latitude}, {random_longitude}, 1, 1, 1, {ride_date}, ST_SetSRID(ST_MakePoint({random_longitude}, {random_latitude}), 4326), ST_MakeLine(ST_SetSRID(ST_MakePoint({random_longitude}, {random_latitude}), 4326), ST_SetSRID(ST_MakePoint({random_longitude}, {random_latitude}), 4326)));"
                    random_latitude  += random.uniform(-0.000001, 0.000001)
                    random_longitude += random.uniform(-0.000001, 0.000001)
                    ride_date = time.strftime('%Y-%m-%d %H:%M:%S.%f')[:3]
                    cursor = connection.cursor()
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    duration += end - start
                    path_length += 1
                end = time.strftime('%Y-%m-%d %H:%M:%S.%f')[:3]
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},1,{initial_start},{end},{duration}\n")
                print(f"Ride {ride_id} of length {path_length} inserted successfully into cycling_data table")
                connection.commit()
            #bulk insert ride_data

            case "bulk_insert_rides":
                cursor = connection.cursor()
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
                        insert_query = f"INSERT INTO cycling_data (ride_id, rider_id, latitude, longitude, x, y, z, timestamp, point_geom, line_geom) VALUES ({ride_id}, {random_rider_id}, {random_latitude}, {random_longitude}, 1, 1, 1, {ride_date}, ST_SetSRID(ST_MakePoint({random_longitude}, {random_latitude}), 4326), ST_MakeLine(ST_SetSRID(ST_MakePoint({random_longitude}, {random_latitude}), 4326), ST_SetSRID(ST_MakePoint({random_longitude}, {random_latitude}), 4326)));"
                        random_latitude  += random.uniform(-0.000001, 0.000001)
                        random_longitude += random.uniform(-0.000001, 0.000001)
                        ride_date = time.strftime('%Y-%m-%d %H:%M:%S.%f')[:3]
                        
                        start = time.time()
                        cursor.execute(query)
                        # get time after executing query
                        end = time.time()
                        duration += end - start
                        path_length += 1
                    end = time.strftime('%Y-%m-%d %H:%M:%S.%f')[:3]
                    rides_inserted += 1
                    ride_id += 1
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},1,{initial_start},{end},{duration}\n")
                print(f"Ride {ride_id} of length {path_length} inserted successfully into cycling_data table")
                connection.commit()

            case "bounding_box":
                cursor = connection.cursor()
                poslong, poslat = generate_random_position_in_Berlin()
                query_addition = f"WHERE ST_Intersects(cycling_data.point_geom::geography, ST_MakeEnvelope({poslong-0.1}, {poslat-0.1}, {poslong+0.1}, {poslat+0.1}, 4326)::geography);"
                query = query + query_addition
                print(query)
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = cursor.fetchall()
                print(records)

            case "polygonal_area":
                cursor = connection.cursor()
                # for now, static polygonal area
                lat1 , lon1 = generate_random_position_in_Berlin()
                lat2 , lon2 = generate_random_position_in_Berlin()
                lat3 , lon3 = generate_random_position_in_Berlin()
                lat4 , lon4 = generate_random_position_in_Berlin()
                query_addition = f"WHERE ST_Intersects(cycling_data.point_geom::geography, ST_GeomFromText('POLYGON(({lon1} {lat1}, {lon2} {lat2}, {lon3} {lat3}, {lon1} {lat1}))', 4326)::geography);"
                query = query + query_addition
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = cursor.fetchall()
                print(records)
            
            #temporal query POSTGIS style
            case "time_interval":
                cursor = connection.cursor()
                # define start and end time for the query
                start_time = "2022-07-01 00:00:00"
                end_time = "2023-07-01 01:00:00"
                query_addition = f"WHERE timestamp BETWEEN '{start_time}' AND '{end_time}' LIMIT {limit};"
                query = query + query_addition
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = cursor.fetchall()
                print(records)

            #MobilityDB feature test
            case "get_trip":
                cursor = connection.cursor()
                # define start and end time for the query
                ride_id = random.randint(1, 400)
                query_addition = f" SELECT asText(trip) AS trip_geom  FROM cycling_trips WHERE ride_id = {ride_id};"
                #dont use default select all
                query = query_addition
                print(query)
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = cursor.fetchall()
                print(records)
            case "get_trip_length":
                cursor = connection.cursor()
                # define start and end time for the query
                ride_id = random.randint(1, 400)
                query_addition = f" SELECT length(trip) FROM cycling_trips WHERE ride_id = {ride_id};"
                #dont use default select all
                query = query_addition
                print(query)
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = cursor.fetchall()
                print(records)
            #MobiilityDB temporal support test
            case "get_trip_duration":
                cursor = connection.cursor()
                # define start and end time for the query
                ride_id = random.randint(1, 400)
                query_addition = f"SELECT duration(trip) FROM cycling_trips WHERE ride_id = {ride_id};"
                #dont use default select all
                query = query_addition
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = cursor.fetchall()
                print(records)
            case "get_trip_speed":
                cursor = connection.cursor()
                # define start and end time for the query
                ride_id = random.randint(1, 400)
                query_addition = f"SELECT speed(trip) FROM cycling_trips WHERE ride_id = {ride_id};"
                #dont use default select all
                query = query_addition
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = cursor.fetchall()
                print(records)
            case "interval_around_timestamp":
                cursor = connection.cursor()
                # define start and end time for the query
                start_time = "2023-07-01 00:00:00"
                query_addition = f"WHERE timestamp BETWEEN '{start_time}' - INTERVAL '1 hour' AND '{start_time} + INTERVAL '1 hour';"
                query = query + query_addition
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = cursor.fetchall()
                print(records)
            case "spatiotemporal":
                cursor = connection.cursor()
                # define start and end time for the query
                start_time = "2023-07-01 00:00:00"
                end_time = "2023-07-01 01:00:00"
                poslong, poslat = generate_random_position_in_Berlin()
                query_addition = f"WHERE timestamp BETWEEN '{start_time}' AND '{end_time}' AND ST_DWithin(cycling_data.point_geom::geography,ST_SetSRID(ST_MakePoint({poslong},{poslat}), 4326)::geography, 5000);"
                query = query + query_addition
                start = time.time()
                cursor.execute(query)
                # get time after executing query
                end = time.time()
                duration = end - start
                # write the duration, along with other query data, to a file
                with open("durations.csv", "a") as file:
                    file.write(f"{query_type},{limit},{start},{end},{duration}\n")
                records = cursor.fetchall()
                print(records)

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            cursor.close()
            connection.close()

# List of queries to execute
def run_threads(query):
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=execute_query, args=(query, query_type, limit))
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
print(ssh_user)
print(ip)
ssh_point = f"ssh {ssh_user}@{ip} '/opt/geomesa-accumulo/bin/geomesa-accumulo export -i test -z localhost -u root  -p test -c example -m -q "" -f ride_data'"
ssh_trip = "ssh $SSH_USER@$GCP_IP '/opt/geomesa-accumulo/bin/geomesa-accumulo export -i test -z localhost -u root  -p test -c example -m -q "" -f trip_data'"



#initial insert of data if not done on machine, 
#clear_table('cycling_data')
#clear_table('cycling_trips')
#initial_insert()



#Configure the benchmark
#run_threads(#Number of parallel threads, default query to use, query type, limit)

benchmark_config = yaml.safe_load(open("benchmark_conf.yaml"))
print(benchmark_config)

#run_threads(2, default_query, "surrounding", 50)
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
