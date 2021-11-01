from pyspark import SparkContext as sc
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType, StringType, StructType, StructField, BooleanType
import ais
import numpy as np

def read(spark, year, month, day):
#     spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "36000").getOrCreate()
    sc = SparkContext.getOrCreate()
    # path = f"LSDE_project/LSDE/2016/04/15"
    path = "2016/04/15"
    
    rdd = sc.wholeTextFiles(path+"/*", minPartitions=None, use_unicode=True)
       
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
#         time_str = time*10**9
            
        
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
                              message_list.append((float(decoded_message["mmsi"]), float(decoded_message["y"]), float(decoded_message["x"])))
                              mmsi_set.add(decoded_message["mmsi"])

              except: continue

        return (time, message_list)

    rdd2 = rdd.map(lambda x: decoder(x))
    rdd3 = rdd2.groupByKey().mapValues(list)
    
    def filter_dups(arr):
                          
        vessel_list = []
        mmsi_set = set()
        def cell_id(lat, lon, time):
            
            lat += 90
            lon += 180
            x_divs_ship = 3600 #360/3
            x_divs_port = 360

            cell_ship = x_divs_ship*int(lat*10)+int(lon*10)
            cell_port = x_divs_port*int(lat)+int(lon)
            
            time = str(time).zfill(4)

            return float(str(cell_ship)+time), cell_port
          
        for frame in arr[1]:
            for vessel in frame:
                try:
                    mmsi = float(vessel[0])
                    if mmsi not in mmsi_set:
                        mmsi_set.add(mmsi)
                        lat, lon = float(vessel[1]), float(vessel[2])
                        cellid_ship, cellid_port = cell_id(lat, lon, arr[0])
                        vessel_list.append([mmsi, lat, lon, float(cellid_ship), float(cellid_port)])
                        
                except:
                    continue

        return (arr[0], vessel_list)
    
    rdd4 = rdd3.map(lambda x: filter_dups(x))
    try:  
        df = rdd4.toDF(["time", "vals"])
    except:
        return None
    df2 = df.select(df.time, explode(df.vals).alias("vals"))
    df3 = df2.select(col("time"), col("vals")[0].alias("MMSI"),col("vals")[1].alias("lat"), col("vals")[2].alias("long"), col("vals")[3].alias("cellid_ship"), col("vals")[4].alias("cellid_port"))
    df4 = df3.withColumn('day', lit(int(day)))
    return df4

def main():
    path = f"LSDE_project/LSDE/2016/04/15"
    
    spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "3600000").getOrCreate()
    
    year = "2016"
    month = "04"
    day = "15"
    
    df = read(spark, year, month, day)
    if df:
        df.orderBy("day").write.partitionBy("day").mode("append").parquet(f"data/15")

if __name__ == "__main__":
 
    

    main()