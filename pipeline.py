from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType, StringType, StructType, StructField, BooleanType
from pyais import decode_msg, FileReaderStream
from geopy import distance
import sys
import numpy as np

def read(spark, cell_map, year, month, day, hour, first_min):
    """
    Takes the current data (year, month, day) and hour, loads in the corresponding text files containing the AIS messages.
    Returns:    dataframe containing the MMSI number, speed, latiitude, longitude of every unique vessel in the file.   
    """

    # Set directory path
    if month < 10:
        month = "0"+str(month)
    path = f"LSDE/{year}/{month}/{day}/"

    # Load files
    if first_min < 10:
        minute = "0"+str(first_min)
    else:
        minute = first_min
    if hour < 10:
        hour = "0"+str(hour)

    try:
        df1 = spark.read.option("sep","\t").csv(path+f"{hour}-{minute}.txt")
    except:
        return None

    for i in range(5):
        try:
            if i+first_min < 10:
                minute = "0"+str(i+first_min)

            df2 = spark.read.option("sep","\t").csv(path+f"{hour}-{minute}.txt")
            df1 = df1.union(df2)
        except:
            pass

    def decode(filename):
        """
        Decodes AIS message. 
        Returns:    array containing the MMSI number, speed, latitude and longitude
        """
        if filename[0] == "!" and int(filename[7]) == 1:
            try:
                decoded_message = decode_msg(filename)
                if int(decoded_message['type']) in [1,2,3]:

                    # Only saves vessels with a valid position and speed under 2 knots.
                    if (float(decoded_message['lat']) != 91.0) and (float(decoded_message['lon']) != 181.0) and (float(decoded_message['speed']) < 2):
                        return [float(decoded_message["mmsi"]), float(decoded_message["lat"]), float(decoded_message["lon"])]
            except: pass

    # Defines user defined function
    msg_cont = udf(decode, ArrayType(FloatType()))

    # Separates array into individual columns and drops duplicated MMSI numbers
    df1 = df1.select(msg_cont("_c0").alias("AIS"))
    df2 = df1.select(col("AIS")[0].alias("MMSI"),col("AIS")[1].alias("lat"), col("AIS")[2].alias("long")).na.drop()
    df2 = df2.groupBy("MMSI").agg(avg("lat").alias("lat"), avg("long").alias("long"))
    if first_min < 10:
        minute = "0"+str(first_min)
    else:
        minute = first_min
    df2 = df2.withColumn('datetime', lit(path+str(hour)+"-"+str(minute)).cast(StringType()))

    broadcastedcells = spark.sparkContext.broadcast(cell_map.collect())

    def groupToCell(arr):
        lat, lon = float(arr[0]), float(arr[1])
        for cell in broadcastedcells.value:
            if (lon > cell[1]) and (lon <= cell[2]) and (lat > cell[3]) and (lat <= cell[4]):
                return cell[0]

    group_to_cell = udf(groupToCell, IntegerType()).asNondeterministic()

    df3 = df2.select(col("datetime"), col("MMSI"), col('lat'), col('long') ,group_to_cell(array(col('lat'), col('long'))).alias('cellid'))
    
    return df3

def ports(spark, cell_width, cell_length):
    # Loads in csv file of all major port coordinates
    dfp = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferschema", "true").load("LSDE/2016/ports.csv*")
    dfp = dfp.select(col("latitude").alias("latp"), col("longitude").alias("longp"))

    port_map_schema = StructType([StructField("cell", IntegerType(), True), \
        StructField("Long_min", FloatType(), True), \
        StructField("Long_max", FloatType(), True), \
        StructField("Lat_min", FloatType(), True), \
        StructField("Lat_max", FloatType(), True), \
        ])
   
    coords = []
    cell_counter = 0
    neighbour_matrix = np.full((int(180/cell_length)+2, int(360/cell_width)), -1)
  
    for i in np.arange(-90,90, cell_length):
        for j in np.arange(-180,180,cell_width):
            coords.append((cell_counter, float(j), float(j+cell_width), float(i), float(i+cell_length)))
            cell_counter += 1

    cell_counter = 0
    for i in range(1, int(180/cell_length)+1):
        for j in range(int(360/cell_width)):
            neighbour_matrix[i, j] = int(cell_counter)
            cell_counter += 1

    cell_map = spark.createDataFrame(data=coords, schema=port_map_schema)
    broadcastedcells = spark.sparkContext.broadcast(cell_map.collect())
    
    # make a list of neighbour tuples
    neighbour_tuples = []
    for i in range(1, neighbour_matrix.shape[0]-1):
        for j in range(neighbour_matrix.shape[1]):

            # to wrap around the longitude
            right = j+1
            if j == int(360/cell_width)-1:
                right = 0

            curr_id = int(neighbour_matrix[i,j])
            neighbour_tuples.append((curr_id, curr_id))
            neighbour_id_1 = int(neighbour_matrix[i-1, j-1])
            neighbour_tuples.append((curr_id, neighbour_id_1))
            neighbour_id_2 = int(neighbour_matrix[i-1, j])
            neighbour_tuples.append((curr_id, neighbour_id_2))
            neighbour_id_3 = int(neighbour_matrix[i-1, right])
            neighbour_tuples.append((curr_id, neighbour_id_3))
            neighbour_id_4 = int(neighbour_matrix[i, j-1])
            neighbour_tuples.append((curr_id, neighbour_id_4))
            neighbour_id_5 = int(neighbour_matrix[i, right])
            neighbour_tuples.append((curr_id, neighbour_id_5))
            neighbour_id_6 = int(neighbour_matrix[i+1, j-1])
            neighbour_tuples.append((curr_id, neighbour_id_6))
            neighbour_id_7 = int(neighbour_matrix[i+1, j])
            neighbour_tuples.append((curr_id, neighbour_id_7))
            neighbour_id_8 = int(neighbour_matrix[i+1, right])
            neighbour_tuples.append((curr_id, neighbour_id_8))

    # make dataframe from neighbour tuples
    neighbour_schema = StructType([StructField("celln", IntegerType(), True), \
        StructField("Neighbour", IntegerType(), True), \
        ])

    dfn = spark.createDataFrame(data=neighbour_tuples, schema=neighbour_schema).where(col("Neighbour") != -1)

    def groupToCell(arr):
        lat, lon = float(arr[0]), float(arr[1])
        for cell in broadcastedcells.value:
            if (lon > cell[1]) and (lon <= cell[2]) and (lat > cell[3]) and (lat <= cell[4]):
                return cell[0]

    group_to_cell = udf(groupToCell, IntegerType()).asNondeterministic()
    
    dfp = dfp.select(col('latp'), col('longp'), group_to_cell(array(col('latp'), col('longp'))).alias('cellp')) \
        .orderBy('cellp')

    return cell_map, dfn, dfp

def filter_port(spark, dfn, dfp, df):
  
    # Acquires neighbouring cells for each vessel
    df = df.join(dfn, on=(col("cellid") == col("celln")), how="inner")

    # Acquires ports in neighbouring cells for each vessel
    df = df.join(dfp, on=(col("neighbour") == col("cellp")), how="left") \
        .select("datetime", "MMSI", "lat", "long", "cellid", "latp", "longp")

    print(df.show(n=100))

    def filterp(arr):
        lat, lon, latp, lonp   = arr[0], arr[1], arr[2], arr[3]
        dis = distance.distance((lat, lon), (latp, lonp)).km
        return dis
    
    port_filter = udf(filterp, FloatType()).asNondeterministic()

    # Calculates distance between vessel and ports in the neighbourhood of the vessel
    dffilter = df.select("datetime", "MMSI", "lat", "long", "cellid", port_filter(array("lat", "long", "latp", "longp")).alias("dis"))

    # filters out all vessels that are within 10km of a port and removes them from df
    dffilter = dffilter.join(dffilter.where(col("dis") < 10).select("MMSI"), on="MMSI", how="leftanti").dropDuplicates(["MMSI"])
    df = dffilter.select("datetime", "MMSI", "cellid", "lat", "long")
   
    return df
    
def get_vessel_pairs(spark, df, dfn):

    # Acquires neighbouring cells for each vessel
    df2 = df.join(dfn, on=(col("cellid") == col("celln")), how="inner").drop(col("cellid"))

    # Creates vessel pairs
    df = df.select(col("MMSI").alias("MMSI2"), col("cellid"), col("lat").alias("lat2"), col("long").alias("long2"))
    df = df2.join(df, on=(col("neighbour") == col("cellid")), how="inner")
    print(df.count())

    # Removes duplicate pairs of vessels
    df = df.where(col("MMSI") > col("MMSI2"))
    
    def filter_pairs(arr):
        lat1, lon1, lat2, lon2   = arr[0], arr[1], arr[2], arr[3]
        dis = distance.distance((lat1, lon1), (lat2, lon2)).km
        return dis
    
    pair_filter = udf(filter_pairs, FloatType()).asNondeterministic()

    # Only keeps vessel pairs which are less than 500m away from each other
    df = df.select("datetime", "MMSI", "MMSI2", "cellid", pair_filter(array("lat", "long", "lat2", "long2")).alias("dis")) \
        .where(col("dis") < 0.5).drop(col("dis"))
    
    return df
    


def main():

    cell_width, cell_length = 0.1, 0.1
    spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "36000").getOrCreate()

    cell_map, dfn, dfp = ports(spark, cell_width, cell_length)

    for year in [2016]:
        for month in [4]:
            for day in [15]:
                for hour in [12]:
                    for minute in [0, 5, 10, 15]:
                        
                        df = read(spark, cell_map, year, month, day, hour, minute)
                        
                        print("HEYOOOO")
                        df = filter_port(spark, dfn, dfp, df)
                        df = get_vessel_pairs(spark, df, dfn)
                        if month < 10:
                            month = int("0"+str(month))
                        if day < 10:
                            day = int("0"+str(day))
                        if hour < 10:
                            hour = int("0"+str(hour))
                        if minute < 10:
                            minute = int("0"+str(minute))                     
                        df.write.mode("overwrite").parquet(f"{year}/{month}/{day}/{hour}-{minute}.parquet")

if __name__ == "__main__":
    main()