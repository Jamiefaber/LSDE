from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType
from pyais import decode_msg
from geopy import distance
import sys

def main ():

    spark = SparkSession.builder.getOrCreate()

    path = "data/"

    df1 = spark.read.option("sep","\t").csv(path+"12-45.txt")
    dfp = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferschema", "true").load(path+"ports.csv*")
    
    dfp = dfp.select(col("latitude").alias("lat"), col("longitude").alias("lon"))

    # df2 = spark.read.option("sep",",").csv(path+"12-46.txt")
    # df3 = spark.read.option("sep",",").csv(path+"12-47.txt")
    # df4 = spark.read.option("sep",",").csv(path+"12-48.txt")
    # df5 = spark.read.option("sep",",").csv(path+"12-49.txt")
    # df6 = spark.read.option("sep",",").csv(path+"12-50.txt")


    def decode(filename):
        if filename[0] == "!" and filename[7] != "2":
            decoded_message = decode_msg(filename)
            if int(decoded_message['type']) in [1,2,3]:
            
                # return f'{decoded_message["mmsi"]}, {decoded_message["speed"]}, {decoded_message["lat"]}, {decoded_message["lon"]}'
                return [float(decoded_message["mmsi"]), float(decoded_message["speed"]), float(decoded_message["lat"]), float(decoded_message["lon"])]

    msg_cont = udf(decode, ArrayType(FloatType()))

    df1 = df1.select(msg_cont("_c0").alias("AIS"))
    df2 = df1.select(col("AIS")[0].alias("MMSI"),col("AIS")[1].alias("speed"),col("AIS")[2].alias("lat"), col("AIS")[3].alias("long"))
    df2 = df2.dropDuplicates(["MMSI"])

    def port_filter(mmsi, lat, lon, dfp):
        def port_loc(x):
            dis = distance.distance((float(x["lat"]), float(x["lon"])), (lat, lon)).km
            if dis > 10:
                return mmsi 
        mmsi = dfp.foreach(port_loc)
        return mmsi
        
    port_loc_check = udf(port_filter, FloatType())
    
    df3 = df2.select(port_loc_check(col("MMSI")).alias("mmsi"), col("lat"), col("long"), dfp)
    df2 = df2.join(df3, on=(col("MMSI") == col("mmsi")))
    

    # print(df1.show(n=5))
    print(df2.show(n=5))

if __name__ == "__main__":
    main()