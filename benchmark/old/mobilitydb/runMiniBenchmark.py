import psycopg2
import threading
import random
import time
import os
import csv
import threading
import sys

hostname = sys.argv[1]
portnum = sys.argv[2]
deployment = "multi"
if len(sys.argv) == 4:
    deployment = sys.argv[3]
default_query = "SELECT * FROM cycling_data "

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


def execute_query(query="SELECT * FROM cycling_data " , query_type = "surrounding", limit = 50, table = "cycling_data", host = hostname, port = "5432", user = "postgres", password = "test"):
    try:

        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="test",
            host=hostname,
            port=portnum
        )
        connection = ""

        query_table = table
        
    #match case in python
        if query_table == "cycling_data":
            match query_type:
                case "insert":
                    cursor = connection.cursor()
                    
                    writes = limit
                    ride_id = get_max_ride_id() + 1
                    ride_id =1
                    rider_id = random.randint(1, 30)
                    latitude = random.uniform(52.338049, 52.675454)
                    longitude = random.uniform(13.088346, 13.761160)
                    multirow_insert = ""
                    for i in range(writes):
                        multirow_insert += "(" + str(ride_id) + ", " + str(rider_id) + ", " + str(latitude) + ", " + str(longitude) + ", 1, 1, 1, '" + time.strftime('%Y-%m-%d %H:%M:%S') + "', ST_SetSRID(ST_MakePoint(" + str(longitude) + ", " + str(latitude) + "), 4326)),\n"
                        
                    multirow_insert = multirow_insert[:-2]
                    query = f"INSERT INTO cycling_data (ride_id, rider_id, latitude, longitude, x, y, z, timestamp, point_geom, line_geom) VALUES {multirow_insert};"
                    #run the query
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    connection.commit()

                    duration = end - start
                    # write the duration, along with other query data, to a file
                    with open("durations.csv", "a") as file:
                        file.write(f"{query_type},{limit},{duration}\n")
                    # commit the transaction

                    
                case "surrounding":
                    poslong, poslat = generate_random_position_in_Berlin()
                    query_addition = f"WHERE ST_DWithin(cycling_data.point_geom::geography,ST_SetSRID(ST_MakePoint({poslong},{poslat}), 4326)::geography, 5000);"
                    cursor = connection.cursor()
                    query = query + query_addition
                    print(query)
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    duration = end - start
                    # write the duration, along with other query data, to a file
                    with open("durations.csv", "a") as file:
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()

                    print(records)
                    
                    # get paths between two random points

                #get intersections of a specific ride, may not make sense with point data
                case "ride_traffic":
                    pass    
                #may also not make sense with point data
                case "intersections":
                    pass
                case "bounding_box":
                    cursor = connection.cursor()
                    poslong, poslat = generate_random_position_in_Berlin()
                    query_addition = f"WHERE ST_Intersects(cycling_data.point_geom::geography, ST_MakeEnvelope({poslong-0.1}, {poslat-0.1}, {poslong+0.1}, {poslat+0.1}, 4326)::geography) LIMIT {limit};"
                    query = query + query_addition
                    print(query)
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    duration = end - start
                    # write the duration, along with other query data, to a file
                    with open("durations.csv", "a") as file:
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()
                    print(records)

                case "polygonal_area":
                    cursor = connection.cursor()
                    # for now, static polygonal area
                    lat1 , lon1 = generate_random_position_in_Berlin()
                    lat2 , lon2 = generate_random_position_in_Berlin()
                    lat3 , lon3 = generate_random_position_in_Berlin()
                    lat4 , lon4 = generate_random_position_in_Berlin()
                    query_addition = f"WHERE ST_Intersects(cycling_data.point_geom::geography, ST_GeomFromText('POLYGON(({lon1} {lat1}, {lon2} {lat2}, {lon3} {lat3}, {lon1} {lat1}))', 4326)::geography) LIMIT {limit};"
                    query = query + query_addition
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    duration = end - start
                    # write the duration, along with other query data, to a file
                    with open("durations.csv", "a") as file:
                        file.write(f"{query_type},{limit},{duration}\n")
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
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()
                    print(records)

                #MobilityDB feature test
                case "interval_around_timestamp":
                    cursor = connection.cursor()
                    # define start and end time for the query
                    start_time = "2023-07-01 00:00:00"
                    query_addition = f"WHERE timestamp BETWEEN '{start_time}' - INTERVAL '1 hour' AND '{start_time} + INTERVAL '1 hour' LIMIT {limit};"
                    query = query + query_addition
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    duration = end - start
                    # write the duration, along with other query data, to a file
                    with open("durations.csv", "a") as file:
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()
                    print(records)
                case "spatiotemporal":
                    cursor = connection.cursor()
                    # define start and end time for the query
                    start_time = "2023-07-01 00:00:00"
                    end_time = "2023-07-01 01:00:00"
                    poslong, poslat = generate_random_position_in_Berlin()
                    query_addition = f"WHERE timestamp BETWEEN '{start_time}' AND '{end_time}' AND ST_DWithin(cycling_data.point_geom::geography,ST_SetSRID(ST_MakePoint({poslong},{poslat}), 4326)::geography, 5000) LIMIT {limit};"
                    query = query + query_addition
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    duration = end - start
                    # write the duration, along with other query data, to a file
                    with open("durations.csv", "a") as file:
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()
                    print(records)
        else:
            #if else not necessary, but for clarity of splitting requests for table
            match query_type:
                
                case "insert":
                    cursor = connection.cursor()
                    writes = limit
                    ride_id = get_max_ride_id() + 1
                    rider_id = random.randint(1, 30)
                    latitude = random.uniform(52.338049, 52.675454)
                    longitude = random.uniform(13.088346, 13.761160)
                    multirow_insert = ""
                    for i in range(writes):
                        multirow_insert += f"({ride_id}, {rider_id}, {latitude}, {longitude}, 1, 1, 1, '{time.strftime('%Y-%m-%d %H:%M:%S')}', ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326), ST_MakeLine(ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326), ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326))),\n"
                    multirow_insert = multirow_insert[:-2]
                    query = f"INSERT INTO cycling_trips (ride_id, rider_id, trip) VALUES {multirow_insert};"
                    #run the query
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    connection.commit()
                    duration = end - start
                    # write the duration, along with other query data, to a file
                    with open("durations.csv", "a") as file:
                        file.write(f"{query_type},{limit},{duration}\n")
                    print("Successfully wrote to the database in ", duration, " seconds")
                case "surrounding":
                    poslong, poslat = generate_random_position_in_Berlin()
                    run_table = query_table
                    if deployment == "multi":
                        run_table += "_ref"
                    query_addition = f"WHERE ST_DWithin({run_table}.trip::geography,ST_SetSRID(ST_MakePoint({poslong},{poslat}), 4326)::geography, 5000);"
                    cursor = connection.cursor()
                    query = query + query_addition
                    print(query)
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    duration = end - start
                    # write the duration, along with other query data, to a file
                    with open("durations.csv", "a") as file:
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()

                    print(records)
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
                        file.write(f"{query_type},{limit},{duration}\n")
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
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()
                    print(records)
                #insert a single trip into the database
                case "bounding_box":
                    cursor = connection.cursor()
                    poslong, poslat = generate_random_position_in_Berlin()
                    run_table = query_table
                    if deployment == "multi":
                        run_table += "_ref"
                    query_addition = f"WHERE ST_Intersects({run_table}.trip::geography, ST_MakeEnvelope({poslong-0.1}, {poslat-0.1}, {poslong+0.1}, {poslat+0.1}, 4326)::geography) LIMIT {limit};"
                    query = query + query_addition
                    print(query)
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    duration = end - start
                    # write the duration, along with other query data, to a file
                    with open("durations.csv", "a") as file:
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()
                    print(records)

                case "polygonal_area":
                    cursor = connection.cursor()
                    # for now, static polygonal area
                    lat1 , lon1 = generate_random_position_in_Berlin()
                    lat2 , lon2 = generate_random_position_in_Berlin()
                    lat3 , lon3 = generate_random_position_in_Berlin()
                    lat4 , lon4 = generate_random_position_in_Berlin()
                    run_table = query_table
                    if deployment == "multi":
                        run_table += "_ref"
                    query_addition = f"WHERE ST_Intersects({run_table}.trip::geography, ST_GeomFromText('POLYGON(({lon1} {lat1}, {lon2} {lat2}, {lon3} {lat3}, {lon1} {lat1}))', 4326)::geography) LIMIT {limit};"
                    query = query + query_addition
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    duration = end - start
                    # write the duration, along with other query data, to a file
                    with open("durations.csv", "a") as file:
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()
                    print(records)
                
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
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()
                    print(records)
                case "interval_around_timestamp":
                    cursor = connection.cursor()
                    # define start and end time for the query
                    start_time = "2023-07-01 00:00:00"
                    query_addition = f"WHERE timestamp BETWEEN '{start_time}' - INTERVAL '1 hour' AND '{start_time} + INTERVAL '1 hour' LIMIT {limit};"
                    query = query + query_addition
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    duration = end - start
                    # write the duration, along with other query data, to a file
                    with open("durations.csv", "a") as file:
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()
                    print(records)
                case "spatiotemporal":
                    cursor = connection.cursor()
                    # define start and end time for the query
                    start_time = "2023-07-01 00:00:00"
                    end_time = "2023-07-01 01:00:00"
                    poslong, poslat = generate_random_position_in_Berlin()
                    run_table = query_table
                    if deployment == "multi":
                        run_table += "_ref"
                    query_addition = f"WHERE timestamp BETWEEN '{start_time}' AND '{end_time}' AND ST_DWithin({run_table}.trip::geography,ST_SetSRID(ST_MakePoint({poslong},{poslat}), 4326)::geography, 5000) LIMIT {limit};"
                    query = query + query_addition
                    start = time.time()
                    cursor.execute(query)
                    # get time after executing query
                    end = time.time()
                    duration = end - start
                    # write the duration, along with other query data, to a file
                    with open("durations.csv", "a") as file:
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()
                    print(records)
                
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
                        file.write(f"{query_type},{limit},{duration}\n")
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
                        file.write(f"{query_type},{limit},{duration}\n")
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
                        file.write(f"{query_type},{limit},{duration}\n")
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
                        file.write(f"{query_type},{limit},{duration}\n")
                    records = cursor.fetchall()
                    print(records)
                
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            cursor.close()
            connection.close()

# List of queries to execute
def run_threads(num_threads, query, query_type, limit, table):
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=execute_query, args=(query, query_type, limit, table))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
        # clear the threads list
        threads.clear()

# Create and start a thread until the number of threads is reached
# run mini benchmark

#initial insert of data if not done on machine, 
#clear_table('cycling_data')
#clear_table('cycling_trips')
#initial_insert()


#Configure the benchmark
#run_threads(#Number of parallel threads, default query to use, query type, limit)
run_threads(2, default_query, "insert_points", 2, "cycling_data")
# run_threads(2, default_query, "surrounding", 50, "cycling_data")
# run_threads(2, default_query, "ride_traffic", 50, "cycling_data")
# run_threads(2, default_query, "intersections", 50, "cycling_data")
# run_threads(2, default_query, "insert_ride", 10, "cycling_data")
# run_threads(1, default_query, "bulk_insert_rides", 10, "cycling_data")
# run_threads(2, default_query, "bounding_box", 50, "cycling_data")
# run_threads(2, default_query, "polygonal_area", 50, "cycling_data")
# run_threads(2, default_query, "time_interval", 50, "cycling_data")
#run_threads(2, default_query, "interval_around_timestamp", 50, "cycling_data")
#run_threads(2, default_query, "spatiotemporal", 50, "cycling_data")

# run_threads(2, default_query, "surrounding", 50, "cycling_trips")
# run_threads(2, default_query, "ride_traffic", 50, "cycling_trips")
# run_threads(2, default_query, "intersections", 50, "cycling_trips")
# run_threads(2, default_query, "insert_ride", 10, "cycling_trips")
# run_threads(1, default_query, "bulk_insert_rides", 10, "cycling_trips")
# run_threads(2, default_query, "bounding_box", 50, "cycling_trips")
# run_threads(2, default_query, "polygonal_area", 50, "cycling_trips")
# run_threads(2, default_query, "time_interval", 50, "cycling_trips")
# run_threads(2, default_query, "get_trip", 50, "cycling_trips")
# run_threads(2, default_query, "get_trip_length", 50, "cycling_trips")
# run_threads(2, default_query, "get_trip_duration", 50, "cycling_trips")
# run_threads(2, default_query, "get_trip_speed", 50, "cycling_trips")
#run_threads(2, default_query, "interval_around_timestamp", 50, "cycling_trips")
#run_threads(2, default_query, "spatiotemporal", 50, "cycling_trips")

