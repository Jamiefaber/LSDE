from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType, StringType, StructType, StructField
from pyais import decode_msg, FileReaderStream
from geopy import distance
import sys
import numpy as np

def read(spark, year, month, day, hour, first_min):
    """
    Takes the current data (year, month, day) and hour, loads in the corresponding text files containing the AIS messages.
    Returns:    dataframe containing the MMSI number, speed, latiitude, longitude of every unique vessel in the file.   
    """

    # Set directory path
    if month < 10:
        month = "0"+str(month)
    path = f"{year}/{month}/{day}/"

    # Load files
    if first_min < 10:
        minute = "0"+str(first_min)
    if hour < 10:
        hour = "0"+str(hour)
    df1 = spark.read.option("sep","\t").csv(path+f"{hour}-{minute}.txt")
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
    df2 = df2.withColumn('datetime', lit(path+str(hour)+"-"+str(minute)).cast(StringType()))
    print(df2.show())
    
    # df2 = df2.withColumn("info", array([array([col('MMSI'), col('speed'), col("lat"), col("long")])])).select("info")
    # df2 = df2.select(flatten("info")).collect()
    # print(df2.show())
    
    return df2

def ports(spark, cell_width, cell_length):
    # Loads in csv file of all major port coordinates
    dfp = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferschema", "true").load("2016/ports.csv*")
    dfp = dfp.select(col("latitude").alias("lat"), col("longitude").alias("lon"))
    # dfp = dfp.withColumn('cell', lit(-1).cast(IntegerType()))

    port_map_schema = StructType([StructField("Cell", IntegerType(), True), \
        StructField("Long_min", IntegerType(), True), \
        StructField("Long_max", IntegerType(), True), \
        StructField("Lat_min", IntegerType(), True), \
        StructField("Lat_max", IntegerType(), True), \
        ])

    coords = []
    cell_counter = 0
    for i in range(-90,90, cell_length):
        for j in range(-180,180,cell_width):
            coords.append((cell_counter, j, j+cell_width, i, i+cell_length))
            cell_counter += 1
    
    cell_map = spark.createDataFrame(data=coords, schema=port_map_schema)
    broadcastedcells = spark.sparkContext.broadcast(cell_map.collect())
    
    # port_schema = StructType()
    # for i in range(int((180/cell_length)*(360/cell_width))):
    #     addon = [StructField(f"{i}", ArrayType(ArrayType(FloatType())), True)]
    #     port_schema = StructType(port_schema.fields + addon)

    def groupToCell(arr):
        lat, lon = float(arr[0]), float(arr[1])
        for cell in broadcastedcells.value:
            if (lon > cell[1]) and (lon <= cell[2]) and (lat > cell[3]) and (lat <= cell[4]):
                return cell[0]

    group_to_cell = udf(groupToCell, IntegerType()).asNondeterministic()
    
    dfp = dfp.select(col('lat'), col('lon'), group_to_cell(array(col('lat'), col('lon'))).alias('cell')) \
        .orderBy('cell')
    
    print(dfp.show())


def main():

    cell_width, cell_length = 5, 5
    spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "36000").getOrCreate()

    # ports(spark, cell_width, cell_length)
    # dfp = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferschema", "true").load("2016/ports.csv*")
    
    # dfp = dfp.select(col("latitude").alias("lat"), col("longitude").alias("lon"))
    # broadcastedports = spark.sparkContext.broadcast(dfp.collect())


    # for year in [2015, 2016, 2017]:
    #     for month in range(1, 13):
    #         for day in range(1, 32):
    #             for hour in range(0, 24):
    #                 read(year, month, day, hour)
    
    df = read(spark, 2016, 4, 15, 12, 0)

    # Creates cell_id for schema
    # tuple_list = []
    # for i in range(int(180/cell_length)):
    #     for j in range(int(360/cell_width)):
    #         tuple_list.append((i,j))

    # schema = StructType([StructField("Date", StringType(), True)])
    # for id in tuple_list:
    #     addon = [StructField(f"{id}", ArrayType(ArrayType(FloatType())), True)]
    #     schema = StructType(schema.fields + addon)

    # df_end = spark.createDataFrame(data=[("15/04/2016", [[22.,21.], [23., 25.]], [[28.,30.]],)], schema=schema)
    # print(df_end.show())

if __name__ == "__main__":
    main()