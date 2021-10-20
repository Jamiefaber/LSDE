from pyspark import SparkContext as sc
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType, StringType, StructType, StructField, BooleanType
from pyais import decode_msg, FileReaderStream
import ais

from geopy import distance
import sys
import numpy as np

def read(broadcastedcells, year, month, day, hour, minute):
    spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "36000").getOrCreate()
    sc = SparkContext.getOrCreate()
    path = f"/mnt/lsde/ais/{year}/0{month}/{day}/{hour}"
    
    rdd = sc.wholeTextFiles(path+f"-{minute}*", minPartitions=None, use_unicode=True)
    
    def decoder(files):
        """
        Decodes AIS message. 
        Returns:    array containing the MMSI number, speed, latitude and longitude
        """
        hour = int(files[0][-9:-7])*100
        minute = int(files[0][-6:-4])
        
        if minute < 15:
            minute = 0
        elif minute < 30:
            minute = 15
        elif minute < 45:
            minute = 30
        else:
            minute = 45
        
        time = hour+minute
        time_arr = (time)*10**9
        
        messages = str(files[1]).splitlines()
        mmsi_set = set()
        message_list = []
        
        for filename in messages:

              try:
                  decoded_message = ais.decode(filename.split(",")[5], 0)
                  if int(decoded_message['id']) in [1,2,3]:
                      # Only saves vessels with a valid position and speed under 2 knots.
                      if (float(decoded_message['y']) != 91.0) and (float(decoded_message['x']) != 181.0) and (float(decoded_message['sog']) < 2):
                          if decoded_message["mmsi"] not in mmsi_set:
                              message_list.append([time_arr+float(decoded_message["mmsi"]), float(decoded_message["y"]), float(decoded_message["x"])])
                              mmsi_set.add(decoded_message["mmsi"])

              except: continue

        return (time, message_list)
    rdd2 = rdd.map(lambda x: decoder(x))
    df = rdd2.toDF(["time", "vals"])
    df2 = df.select(df.time, explode(df.vals).alias("vals"))
    df3 = df2.select(col("vals")[0].alias("MMSI"),col("vals")[1].alias("lat"), col("vals")[2].alias("long")).na.drop()
#     df4 = df3.groupBy("MMSI").agg(avg("lat").alias("lat"), avg("long").alias("long"))
    df4 = df3.dropDuplicates(["MMSI"])
  
    def groupToCell(arr):
        lat, lon = float(arr[0]), float(arr[1])
        for cell in broadcastedcells.value:
            if (lon > cell[1]) and (lon <= cell[2]) and (lat > cell[3]) and (lat <= cell[4]):
                return cell[0]

    group_to_cell = udf(groupToCell, IntegerType()).asNondeterministic()

    df5 = df4.select(col("MMSI"), group_to_cell(array(col('lat'), col('long'))).alias('cellid'))

    return df4
    
    return

def main():
    path = f"/mnt/group05/utils/"

    cell_map = spark.read.format("parquet").option("header", "true").option("inferschema", "true").load(path + "cell_map")
    broadcastedcells = spark.sparkContext.broadcast(cell_map.collect())
    year = 2016
    month = 4
    day = 15
    
    # Create empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()

    # Create empty schema
    columns = StructType([ \
            StructField("MMSI", FloatType(), True), \
            StructField("lat", FloatType(), True), \
            StructField("long", FloatType(), True), \
    ])

    # Create an empty RDD with empty schema
    df1 = spark.createDataFrame(data = emp_RDD,
                            schema = columns)
    for hour in range(24):
        if hour < 10:
            hour = f"0{hour}"
        for minute in range(6):
            df = read(broadcastedcells, year, month, day, hour, minute)
            df1 = df1.union(df)
    print(df1.show())


if __name__ == "__main__":
 


    main()