import pandas as pd
import glob
from datetime import datetime
import os
import csv
import random
import sys
import utm
csv.field_size_limit(sys.maxsize)

def mergeFiles(directory_path):
    files = glob.glob("../raw/"+ directory_path + '/*', recursive=True)
    lineCounter = 0
    timestamp = 0
    for filename in files:
        #strip the filename of the path and the extension
        rawFileName = os.path.basename(filename)[:-4]
        #remove the last four characters of the filename, which are the file extension
        #check if rawFileName is already in the processed directory
        #if not os.path.exists("../processed/aviation/" + rawFileName + ".csv"):
        with open(filename, 'r') as file:
            print("Processing file: " + rawFileName)
            
            mergedData = []
            count = 0
            for line in file:
                #skip empty lines
                if not line.strip():
                    continue
                #split the line into a list
                line = line.split(",")
                #if the number of elements in the line is larger than 4, the line is a header
                if len(line)  == 4:
                    #append the line to the mergedData list, along with flightID, aircraftType, originAirport, destinationAirport
                    #create a timestamp using the startDate and adding the value of line[0] as seconds to the timestamp
                    #convert startDate to an actual timestamp, which right now is in the format "2023-01-02 18:17:18"

                    #add the value of line[0] as seconds to the timestamp and set it as currentTimestamp. Only the value before the period in line[0] is used

                    currentTimestamp = timestamp + pd.Timedelta(seconds=int(line[0].split(".")[0]))

                    
                    #save current Timestamp as DD-MM-YYYY HH:MM:SS
                    #convert utm coordinates to lat/long
                    latitude, longitude = utm.to_latlon(float(line[1]), float("5"+line[2]), 32, 'U')
                    #write the gathered values as a single line to mergedData, along with the current timestamp, UTM coordinates which are found in line[1] and line[2], and altitude (line[3]) to the mergedData list, and strip line[3] of the newline character
                    mergedData+= [[flightID, aircraftType, originAirport, destinationAirport, currentTimestamp, latitude,longitude, line[3].strip()]]                  
                else:
                    #check if mergedData includes data
                    flightID = line[0]
                    aircraftType = line[1]
                    originAirport = line[2]
                    destinationAirport = line[3]
                    startDate = line[6] + " " + line[7]
                    timestamp = datetime.strptime(startDate, "%Y-%m-%d %H:%M:%S")
                    lineCounter = int(line[10])
                    
        #after reading the file, write the processed data to a new file in the processed directory
        with open("../processed/aviation/" +rawFileName+ ".csv", 'w') as file:
            writer = csv.writer(file)
            writer.writerows(mergedData)
            mergedData = []
def createPointData(directory_path):
    files = glob.glob("../processed/" + directory_path + '/NRW*')
    for filename in files:
        print("Processing file: " + filename)
        firstLine = True
        startTime = 0;
        startAltitude = 0;
        startLongitude = 0;
        startLatitude = 0;
        flightID = 0;
        aircraftType = 0;
        originAirport = 0;
        destinationAirport = 0;
        data = []
        #for each line in the file
        for line in open(filename, 'r'):
            #skip the first line

            line = line.split(",")
            firstLine = False
            flightID = line[0]
            aircraftType = line[1]
            originAirport = line[2]
            destinationAirport = line[3]
            timestamp = line[4]
            latitude = line[5]
            longitude = line[6]
            startAltitude = line[7]
            #split the line into a list
            #create a linestring from the points
            linestring = f"POINT({longitude} {latitude})"
            #remove quotes from the linestring
            #write the gathered values as a single line to data,
            data += [[flightID, aircraftType, originAirport, destinationAirport, linestring.strip(), timestamp, startAltitude.strip()]]
            startLongitude = line[6]
            startLatitude = line[5]
            startAltitude = line[7] 
            startTime = line[4]
        #once done with the file, write the data to a new file called "trips_"+filename
        with open("../processed/aviation/point_" + os.path.basename(filename), 'w') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerows(data)
            data = []
def createTrajData(directory_path):
    #open the processed point files
    files = glob.glob("../processed/" + directory_path + '/NRW*')
    #for each file in the processed directory
    for filename in files:
        print("Processing file: " + filename)
        firstLine = True
        startTime = 0;
        startAltitude = 0;
        startLongitude = 0;
        startLatitude = 0;
        flightID = 0;
        aircraftType = 0;
        originAirport = 0;
        destinationAirport = 0;
        data = []
        #for each line in the file
        for line in open(filename, 'r'):
            #skip the first line
            if firstLine:
                line = line.split(",")
                firstLine = False
                flightID = line[0]
                aircraftType = line[1]
                originAirport = line[2]
                destinationAirport = line[3]
                startTime = line[4]
                startLatitude = line[5]
                startLongitude = line[6]
                startAltitude = line[7]
                continue
            #split the line into a list
            line = line.split(",")
            if line[0] == flightID:
                #create a linestring from the points
                linestring = f"LINESTRING({startLongitude} {startLatitude}, {line[6]} {line[5]})"
                #remove quotes from the linestring
                #write the gathered values as a single line to data,
                data += [[flightID, aircraftType, originAirport, destinationAirport, linestring.strip(), startTime, line[4], startAltitude.strip(), line[7].strip()]]
                startLongitude = line[6]
                startLatitude = line[5]
                startAltitude = line[7] 
                startTime = line[4]
            else: 
                flightID = line[0]
                aircraftType = line[1]
                originAirport = line[2]
                destinationAirport = line[3]
                startTime = line[4]
                startLatitude = line[5]
                startLongitude = line[6]
                startAltitude = line[7]
                continue
        #once done with the file, write the data to a new file called "trips_"+filename
        with open("../processed/aviation/traj_" + os.path.basename(filename), 'w') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerows(data)
            data = []
def createTripData(directory_path):
    #open the processed point files
    files = glob.glob("../processed/" + directory_path + '/NRW*')
    #for each file in the processed directory
    for filename in files:
        print("Processing file: " + filename)
        firstLine = True
        startTime = 0;
        startAltitude = 0;
        startLongitude = 0;
        startLatitude = 0;
        flightID = 0;
        aircraftType = 0;
        originAirport = 0;
        destinationAirport = 0;
        data = []
        linestring = "LINESTRING("
        #for each line in the file
        for line in open(filename, 'r'):
            #skip the first line
            if firstLine:
                line = line.split(",")
                firstLine = False
                flightID = line[0]
                aircraftType = line[1]
                originAirport = line[2]
                destinationAirport = line[3]
                startTime = line[4]
                startLatitude = line[5]
                startLongitude = line[6]
                startAltitude = line[7]
                linestring += f"{startLongitude} {startLatitude}, "
                continue
            #split the line into a list
            line = line.split(",")
            if line[0] == flightID:
                #create a linestring from the points
                linestring += f"{line[6]} {line[5]}, "
                #remove quotes from the linestring
                #write the gathered values as a single line to data,
            else: 
                if linestring.count(",") > 1:
                    if linestring != "LINESTRING(":
                        linestring = linestring[:-2] + ")"
                        data += [[flightID, aircraftType, originAirport, destinationAirport, linestring.strip(), startTime, line[4], startAltitude.strip(), line[7].strip()]]
                linestring = "LINESTRING("
                flightID = line[0]
                aircraftType = line[1]
                originAirport = line[2]
                destinationAirport = line[3]
                startTime = line[4]
                startLatitude = line[5]
                startLongitude = line[6]
                startAltitude = line[7]
                linestring += f"{startLongitude} {startLatitude}, "
        #once done with the file, write the data to a new file called "trips_"+filename
        with open("../processed/aviation/trip_" + os.path.basename(filename), 'w') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerows(data)
            data = []
#mergeFiles("aviation")
createPointData("aviation")
#createTrajData("aviation")
#createTripData("aviation")