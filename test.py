from pyspark import SparkContext as sc
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType, StringType, StructType, StructField, BooleanType
from pyais import decode_msg, FileReaderStream
import ais

from geopy import distance
import sys
import numpy as np

def read(year, month, day):
    spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "36000").getOrCreate()
    sc = SparkContext.getOrCreate()
    path = f"{year}/0{month}/{day}"
    rdd = sc.wholeTextFiles(path, minPartitions=None, use_unicode=True)
    def decoder(files):
        """
        Decodes AIS message. 
        Returns:    array containing the MMSI number, speed, latitude and longitude
        """
        hour = int(files[0][-9:-7])*100
        minute = int(files[0][-6:-4])

        if minute < 5:
            minute = 0
        elif minute < 10:
            minute = 5
        else:
            minute = 10

        
        time = hour+minute

        messages = str(files[1]).splitlines()
        message_list = []
        
        for filename in messages:

            # if filename[0] == "!" and int(filename[7]) == 1:
            try:
                decoded_message = ais.decode(filename.split(",")[5], 0)
                if int(decoded_message['id']) in [1,2,3]:

                    # Only saves vessels with a valid position and speed under 2 knots.
                    if (float(decoded_message['y']) != 91.0) and (float(decoded_message['x']) != 181.0) and (float(decoded_message['sog']) < 2):
                        message_list.append([float(decoded_message["mmsi"]), float(decoded_message["y"]), float(decoded_message["x"])])
        
            except: continue

            if len(message_list) == 2:
                return (time, message_list)
 

        return (time, message_list)
    rdd2 = rdd.map(lambda x: decoder(x))

    rdd3 = rdd2.groupByKey().mapValues(list)


    def filter_dups(arr):
        mmsi_set = set()
        ar = np.array(arr[1])
        ar = ar.reshape(-1, ar.shape[-1])
        
    
        return (arr[0], list(ar))
    
    rdd4 = rdd3.map(lambda x: filter_dups(x))
    print(rdd4.take(2))

    # df = rdd2.toDF(["time", "vals"])


    

    # df2 = df.select(df.time, explode(df.vals).alias("vals"))
    # df3 = df2.select(col("time"), col("vals")[0].alias("MMSI"),col("vals")[1].alias("lat"), col("vals")[2].alias("long")).na.drop()
    # df3 = df3.withColumn("time", df["time"].cast(IntegerType()))
    
    # df2 = df.withColumn("first_time", col("time") - (col("time") % 5)) \
    #     .withColumn("first_time", col("first_time")) \
    #     .groupBy(col("first_time")) \
    #     .agg(flatten(collect_list(col("vals"))).alias("vals"))


        # //change it to needed agg function or anything
    # df2 = df.select(df.time, explode(df.vals).alias("vals"))
    # df3 = df2.select(col("time"), col("vals")[0].alias("MMSI"),col("vals")[1].alias("lat"), col("vals")[2].alias("long")).na.drop()
    # df4 = df3.rollup(col("time"), col("MMSI")).agg(avg("lat").alias("lat"), avg("long").alias("long"))
    # df4 = df3.groupBy(col("time"), col("MMSI")).agg(avg("lat").alias("lat"), avg("long").alias("long"))
    # print(df2.show())
    
    return

def main():
    year = 2016
    month = 4
    day = 15
    read(year, month, day)


if __name__ == "__main__":
 


    main()