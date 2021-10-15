import os
import sys

from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType
from pyais import decode_msg
from geopy import distance
import sys
import numpy as np

def main ():

    spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "360000").config("spark.executor.heartbeatInterval","3600000").config("spark.network.timeout", "3600000").getOrCreate()

    path = "data/"

    df1 = spark.read.option("sep","\t").csv("2016/04/15/13-00.txt")
    dfp = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferschema", "true").load("2016/ports.csv*")
    
    dfp = dfp.select(col("latitude").alias("lat"), col("longitude").alias("lon"))
    broadcastedports = spark.sparkContext.broadcast(dfp.collect())

    # df2 = spark.read.option("sep",",").csv(path+"12-46.txt")
    # df3 = spark.read.option("sep",",").csv(path+"12-47.txt")
    # df4 = spark.read.option("sep",",").csv(path+"12-48.txt")
    # df5 = spark.read.option("sep",",").csv(path+"12-49.txt")
    # df6 = spark.read.option("sep",",").csv(path+"12-50.txt")


    def decode(filename):
        if filename[0] == "!" and int(filename[7]) == 1:
            try:
                decoded_message = decode_msg(filename)
                if int(decoded_message['type']) in [1,2,3]:
                    if (float(decoded_message['lat']) != 91.0) and (float(decoded_message['lon']) != 181.0) and (float(decoded_message['speed']) < 2):
                    # return f'{decoded_message["mmsi"]}, {decoded_message["speed"]}, {decoded_message["lat"]}, {decoded_message["lon"]}'
                        return [float(decoded_message["mmsi"]), float(decoded_message["speed"]), float(decoded_message["lat"]), float(decoded_message["lon"])]
            except: pass

    msg_cont = udf(decode, ArrayType(FloatType()))

    df1 = df1.select(msg_cont("_c0").alias("AIS"))
    df2 = df1.select(col("AIS")[0].alias("MMSI"),col("AIS")[1].alias("speed"),col("AIS")[2].alias("lat"), col("AIS")[3].alias("long"))
    df2 = df2 \
        .dropDuplicates(["MMSI"]) \
        .na.drop()
    print(df2.count())

    def port_filter(arr):
        mmsi, lat, lon = arr[0], arr[1], arr[2]

        for port in broadcastedports.value:
            port_lat = float(port[0])
            port_lon = float(port[1])
            # if (np.abs(port_lat-lat) < 0.09) and (np.abs(port_lon-lon) < 0.09):
            dis = distance.distance((port_lat, port_lon), (lat, lon)).km
            if dis < 10:
                return None
        return mmsi
        
        
    port_loc_check = udf(port_filter, FloatType())
    
    df3 = df2.select(port_loc_check(array(col("MMSI"), col("lat"), col("long"))).alias("mmsinr"))
    df4 = df2.join(df3, on=(col("MMSI") == col("mmsinr"))) \
        .drop(col("mmsinr"))
    print(df4.count())
    dfs = df4.select(col("MMSI"), col("lat"), col("long"))
    broadcastedships = spark.sparkContext.broadcast(dfs.collect())

    def encounter(arr):
        mmsi, lat, lon  = arr[0], arr[1], arr[2]
        neighbours = []
        for ship in broadcastedships.value:
            ship_mmsi = ship[0]
            if mmsi != ship_mmsi:
                ship_lat = ship[1]
                ship_lon = ship[2]
                if (np.abs(ship_lat-lat) < 0.009) and (np.abs(ship_lon-lon) < 0.05):
                    dis = distance.distance((ship_lat, ship_lon), (lat, lon)).km
                    if dis <= 0.5:
                        neighbours.append(ship_mmsi)
        if len(neighbours) == 0:
            return None
        return neighbours

    encounter_finder = udf(encounter, ArrayType(FloatType()))
    df5 = df4.select(col("MMSI"), col("lat"), col("long"), encounter_finder(array(col("MMSI"), col("lat"), col("long"))).alias("neighbours")) \
        .na.drop()

    print(df5.show())
    # print(df5.count())

if __name__ == "__main__":
    main()