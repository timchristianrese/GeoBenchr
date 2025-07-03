import pandas as pd
import geopandas as gpd
from shapely import wkt
import glob 
import os
# Step 1: Read the CSV
def convertCSV(directory_path,has_header,headers, delimiter =","):
    # Read all CSV files in the directory
    files = glob.glob(directory_path + '*.csv')
    dataframes = []
    counter = 0
    for file in files:
        df = ""
        geo_column = ""
        if has_header:
            df = pd.read_csv(file, delimiter=delimiter)
            geo_column = headers
        else:
            df = pd.read_csv(file,header=None, delimiter=delimiter)
            df.columns = headers
            geo_column = "wkt"
        # Convert WKT to geometry
        #check name of third column
        print(df.columns)

        df[geo_column] = df[geo_column].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry=geo_column, crs='EPSG:4326') 

        gdf.to_parquet(directory_path+str(counter)+".geoparquet", engine='pyarrow', index=False)
        counter += 1

#convertCSV("../processed/ais/point", True, "wkt")
#convertCSV("../processed/cycling/point", False, ["ride_id", "rider_id", "wkt", "timestamp"], ",")
convertCSV("../processed/aviation/point", False, ["flightid","aircraft_type","origin","destination","wkt","timestamp","altitude"], ";")

