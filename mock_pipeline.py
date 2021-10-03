from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType
from pyais import decode_msg
from geopy import distance
import csv
import sys

def main ():

    spark = SparkSession.builder.getOrCreate()

    path = "LSDE/data/"

    df1 = spark.read.option("sep","\t").csv(path+"12-45.txt")
    # dfp = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferschema", "true").load(path+"ports.csv*")
    
    # dfp = dfp.select(col("latitude").alias("lat"), col("longitude").alias("lon"))

    # save the port coords in list, there aren't many anyway
    port_coords = []
    with open(path+"ports.csv", 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            port_coords.append((row[3], row[4]))
    port_coords.pop(0)
    # print(port_coords[:10])
    # sys.exit()
    # df2 = spark.read.option("sep",",").csv(path+"12-46.txt")
    # df3 = spark.read.option("sep",",").csv(path+"12-47.txt")
    # df4 = spark.read.option("sep",",").csv(path+"12-48.txt")
    # df5 = spark.read.option("sep",",").csv(path+"12-49.txt")
    # df6 = spark.read.option("sep",",").csv(path+"12-50.txt")


    def decode(filename):
        if filename[0] == "!" and filename[7] != "2":
            
            try:
                decoded_message = decode_msg(filename)
                if int(decoded_message['type']) in [1,2,3]:
                    if str(decoded_message['status']) != 'NavigationStatus.Moored':
                    
                        if (float(decoded_message["lat"]) <= 90) and (float(decoded_message["lat"]) >= -90):
            
                            # return f'{decoded_message["mmsi"]}, {decoded_message["speed"]}, {decoded_message["lat"]}, {decoded_message["lon"]}'
                            return [float(decoded_message["mmsi"]), float(decoded_message["speed"]), float(decoded_message["lat"]), float(decoded_message["lon"])]
            except: pass

    msg_cont = udf(decode, ArrayType(FloatType()))

    df1 = df1.select(msg_cont("_c0").alias("AIS"))
    df2 = df1.select(col("AIS")[0].alias("MMSI"),col("AIS")[1].alias("speed"),col("AIS")[2].alias("lat"), col("AIS")[3].alias("long"))
    df2 = df2.dropDuplicates(["MMSI"])

    # def port_filter(mmsi, lat, lon):
    #     def port_loc(x):
    #         dis = distance.distance((float(x["lat"]), float(x["lon"])), (lat, lon)).km
    #         if dis > 10:
    #             return mmsi 
    #     mmsi = dfp.foreach(port_loc)
    #     return mmsi
        
    # port_loc_check = udf(port_filter, FloatType())
    print(df2.show())
    print(df2.count())
    df2 = df2.dropna().filter(col('speed') < 2)
    print(df2.show())
    print(df2.count())

    valid_mmsi = []
    mmsi_counter = 0
    def loop_function(x):
        switch = 0
        for i in range(len(port_coords)):
            # print(float(x["lat"]), float(x["long"]), port_coords[i])
            # dis = distance.distance((float(x["lat"]), float(x["long"])), port_coords[i]).km
            vars1, vars2 = (float(x["lat"]), float(x["long"])), port_coords[i]
            dis = 11
            if dis > 10:
                valid_mmsi.append(x['MMSI'])
                # mmsi_counter += 1
                # print(int(x['MMSI']))


    df2.foreach(loop_function)
    print(valid_mmsi)
    print(mmsi_counter)
    sys.exit()
    df3 = df2.select(port_loc_check(col("MMSI")).alias("mmsi"), col("lat"), col("long"))
    df2 = df2.join(df3, on=(col("MMSI") == col("mmsi")))
    

    # print(df1.show(n=5))
    print(df2.show(n=5))

if __name__ == "__main__":
    main()