import pandas as pd
import glob
from datetime import datetime
import os
import csv
import random
import sys
csv.field_size_limit(sys.maxsize)

ride_id = 0
def merge_files(directory_path):
    merged_data = []
    #find all files starting with "VM" in the directory and all subdirectories
    files = glob.glob("../raw/"+directory_path + '/*', recursive=True)
    file_count = 0
    merge_count = 0
    ride_id = 0
    for filename in files:
        if filename.__contains__("Profiles"):
            continue
        with open(filename, 'r') as file:
            rider_id = random.randint(1, 30)
            count = 0
            for line in file:
                if line.startswith("lat"):
                    break
                count+=1
            file_data = file.readlines()[count:]
            reader = csv.reader(file_data)
            data = list(reader)
            # create skipper variable to ignore already interpolated data
            skipper = -1
            for i in range(0, len(data)):
                if i >= skipper:
                    if data[i][0] and data[i][1]:
                        for j in range (i, len(data)):
                            if j-i > 1:
                                if data[j][0] and data[j][1]:
                                    skipper = j
                                    start_val0= float(data[i][0])
                                    end_val0 = float(data[j][0])
                                    start_val1= float(data[i][1])
                                    end_val1 = float(data[j][1])
                                    diff0 = end_val0 - start_val0
                                    diff1 = end_val1 - start_val1
                                    num_rows = j-i
                                    increment0 = diff0 / (num_rows)
                                    increment1 = diff1 / (num_rows)
                                    for k in range(1, num_rows):
                                        value0 = round(start_val0 + k * increment0,8)
                                        value1 = round(start_val1 + k * increment1,8)
                                        data[i+k][0] = str(value0)
                                        data[i+k][1] = str(value1)
                                    break  
            for i in range(0, len(data)):
                if not data[i][0] and not data[i][1]:
                    data [i] = ""
                #shift all data in list 2 spaces to right and fill first two spaces with data from ride_id and rider_id
                else:
                    data[i] = [ride_id, rider_id] + data[i][0:2] + [data[i][5]]

            
            # Append the data to the merged_data list
            merged_data.extend(data)
            #write to global variable
            ride_id +=1

            if file_count < 200:
                file_count += 1
            else: 
                with open('../processed/cycling/merged0'+str(merge_count)+'.csv', 'w', newline='') as write_file:
                    writer = csv.writer(write_file)
                    writer.writerows(merged_data)
                    print("Wrote data to merged0"+str(merge_count)+".csv")
                    file_count = 0
                    merge_count += 1
                    merged_data = []
    if file_count != 0:
        with open('../processed/cycling/merged0'+str(merge_count)+'.csv', 'w', newline='') as write_file:
            writer = csv.writer(write_file)
            writer.writerows(merged_data)
            print("Wrote data to merged0"+str(merge_count)+".csv")
            file_count = 0
            merge_count += 1
            merged_data = []

    # Write the merged data to a new CSV file with comma as the delimiter

def trim_csv_files():
    # get all files in the folder starting with merged, but only in the top level directory
    files = glob.glob('../processed/cycling/merged*')
    print(len(files))
    for input_file in files:
        print(f'Trimming file: {input_file}')
        (pd.read_csv(input_file, delimiter=',', header=None, skip_blank_lines=True, usecols= range(5) )).to_csv(input_file, index=False, header= None)
def convert_timestamp():
    # Read the csv file
    files = glob.glob('../processed/cycling/merged*')
    for input_file in files:
        df = pd.read_csv(input_file, header= None)

        # Convert the timestamp to a human readable format
        df[4] = pd.to_datetime(df[4], unit='ms')

        # Write the trimmed dataframe to a new csv file
        df.to_csv(input_file, index=False, header= None)
def createPointData(directory_path):
    files = glob.glob("../raw/" + directory_path + '/merged*')

    for filename in files:
        print("Processing file: " + filename)
        firstLine = True
        startTime = 0;
        startLongitude = 0;
        startLatitude = 0;
        rideID = 0;
        riderID = 0;
        data = []
        #for each line in the file
        for line in open(filename, 'r'):
            line = line.split(",")
            ride_id = line[0]
            rider_id = line[1]
            latitude = line[2]
            longitude = line[3]
            timestamp = line[4]
            #split the line into a list
            #write the gathered values as a single line to data,
            pointValue = f"POINT({longitude} {latitude})"
            data += [[rideID, riderID, pointValue, timestamp.strip()]]
        #once done with the file, write the data to a new file called "trips_"+filename
        with open("../processed/cycling/point_" + os.path.basename(filename), 'w') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerows(data)
            data = []
def createTrajData(directory_path):
    #open the processed point files
    files = glob.glob("../raw/" + directory_path + '/merged*')
    #for each file in the processed directory
    for filename in files:
        print("Processing file: " + filename)
        print("Creating Traj data"+filename)
        firstLine = True
        startTime = 0;
        startLongitude = 0;
        startLatitude = 0;
        rideID = 0;
        riderID = 0;
        data = []
        #for each line in the file
        for line in open(filename, 'r'):
            #skip the first line
            if firstLine:
                line = line.split(",")
                firstLine = False
                rideID = line[0]
                riderID = line[1]
                startTime = line[4]
                startLatitude = line[2]
                startLongitude = line[3]
                continue
            #split the line into a list
            line = line.split(",")
            if line[0] == rideID:
                #create a linestring from the points
                linestring = f"LINESTRING({startLongitude} {startLatitude}, {line[3]} {line[2]})"
                #remove quotes from the linestring
                #write the gathered values as a single line to data,
                data += [[rideID, riderID, linestring.strip(), startTime.strip(), line[4].strip()]]
            else: 
                rideID = line[0]
                riderID = line[1]
                startTime = line[4]
                startLatitude = line[2]
                startLongitude = line[3]
                continue
        #once done with the file, write the data to a new file called "trips_"+filename
        with open("../processed/cycling/traj_" + os.path.basename(filename), 'w') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerows(data)
            data = []
def createTripData(directory_path):
    #open the processed point files
    files = glob.glob("../raw/" + directory_path + '/merged*')
    #for each file in the processed directory
    for filename in files:
        print("Processing file: " + filename)
        firstLine = True
        startTime = 0;
        startLongitude = 0;
        startLatitude = 0;
        rideID = 0;
        riderID = 0;
        data = []
        linestring = "LINESTRING("
        #for each line in the file
        for line in open(filename, 'r'):
            #skip the first line
            if firstLine:
                line = line.split(",")
                firstLine = False
                rideID = line[0]
                riderID = line[1]
                startTime = line[4]
                startLatitude = line[2]
                startLongitude = line[3]
                linestring += f"{startLongitude} {startLatitude}, "
                continue
            #split the line into a list
            line = line.split(",")
            if line[0] == rideID:
                #create a linestring from the points
                linestring += f"{line[3]} {line[2]}, "
                #remove quotes from the linestring
                #write the gathered values as a single line to data,
            else: 
                if linestring != "LINESTRING(":
                    linestring = linestring[:-2] + ")"
                    data += [[rideID, riderID, linestring.strip(), startTime.strip(), line[4].strip()]]
                linestring = "LINESTRING("
                rideID = line[0]
                riderID = line[1]
                startTime = line[4]
                startLatitude = line[2]
                startLongitude = line[3]
                continue
        #once done with the file, write the data to a new file called "trips_"+filename
        with open("../processed/cycling/trip_" + os.path.basename(filename), 'w') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerows(data)
            data = []

    
       
# def create_postgis_trip_data():
#     files = glob.glob('geomesa_trips*')
#     for input_file in files:
#         output_file = 'postgis_trips_' + input_file[-12:]
#         with open(input_file, 'r') as file:
#             write_data = ""
#             #read as csv file
#             filedata = csv.reader(file, delimiter=';')
#             for line in filedata:
#                 timearray = "{"+line[3]+"}"
#                 #write the data to the new file
#                 write_data += f"{line[0]};{line[1]};{line[2]};{timearray}\n"
#             with open(output_file, 'a') as file:
#                 file.write(write_data)

#merge_files("cycling")
#trim_csv_files()
#convert_timestamp()
createPointData("cycling")
#createTrajData("cycling")
#createTripData("cycling")

#create_postgis_trip_data()
