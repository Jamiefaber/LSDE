from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType, StringType, StructType, StructField, BooleanType
from geopy import distance
import numpy as np

def filter_port(spark, dfp, df):
  
#     df = df.join(dfn, on=(col("cellid") == col("celln")), how="inner")

    # Acquires ports in neighbouring cells for each vessel
    df2 = df.join(dfp, on=(col("cellid_port") == col("cellp")), how="left") \
        .select("day", "time", "MMSI", "lat", "long", "cellid_ship", "coords")

    def filterp(arr):
#         dis = 11.0
        
        lat = float(arr[0][0][0])
        lon = float(arr[1][0][0])
        coords = arr[2]
        if coords:
            for coord in coords:

#                 dis = distance.distance((lat, lon), (coord[0], coord[1])).km
                if (np.abs(coord[0]-lat) < 0.09) and (np.abs(coord[1]-lon) < 0.09):
#                     if dis < 10:
                      return None
              
        return 11.0
    
    port_filter = udf(filterp, FloatType()).asNondeterministic()

    # Calculates distance between vessel and ports in the neighbourhood of the vessel
    dffilter = df2.select("day", "time", "MMSI", "lat", "long", "cellid_ship", port_filter(array(array(array("lat")), array(array("long")), "coords")).alias("dis"))
    
    dffilter2 = dffilter.na.drop()
  
    df = dffilter2.select("day", "time", "MMSI", "cellid_ship", "lat", "long")
   
    return df

def get_vessel_pairs(spark, df):

    # Acquires neighbouring cells for each vessel
#     df2 = df.join(dfn, on=(col("cellid") == col("celln")), how="inner").drop(col("cellid"))

    # Creates vessel pairs
    df2 = df.select(col("MMSI").alias("MMSI2"), col("cellid_ship").alias("cellid_ship2"), col("lat").alias("lat2"), col("long").alias("long2"))
    df = df2.join(df, on=(col("cellid_ship") == col("cellid_ship2")), how="inner")

    # Removes duplicate pairs of vessels
    df = df.where(col("MMSI") > col("MMSI2"))
    
#     print(df.count())
    
    def filter_pairs(arr):
        lat1, lon1, lat2, lon2   = arr[0], arr[1], arr[2], arr[3]
        dis = 1.0
        if (np.abs(lat1-lat2) < 0.009) and (np.abs(lon1-lon2) < 0.05):
            dis = distance.distance((lat1, lon1), (lat2, lon2)).km
        return dis
    
    pair_filter = udf(filter_pairs, FloatType()).asNondeterministic()

    # Only keeps vessel pairs which are less than 500m away from each other
    df = df.select("day", "time", "MMSI", "MMSI2", "cellid_ship", pair_filter(array("lat", "long", "lat2", "long2")).alias("dis")) \
        .where(col("dis") < 0.5).drop(col("dis"))
    
    return df

def main():
    spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "360000").getOrCreate()
    
    path = "/mnt/group05/2016/"
  
    dfp = spark.read.format("parquet").option("header", "true").option("inferschema", "true").load("ports")
    
    df = spark.read.format("parquet").option("header", "true").option("inferschema", "true").load("data/15")
    df = df.where((col("lat") <= 90) & (col("lat") >= -90) & (col("long") <= 180) & (col("long") >= -180))
    
    df1 = filter_port(spark, dfp, df.where(col("day") == 15))
#     df1.write.mode("overwrite").parquet(f"/mnt/group05/test/pairs")
#     df1 = spark.read.format("parquet").option("header", "true").option("inferschema", "true").load("/mnt/group05/test/pairs")
    
    df2 = get_vessel_pairs(spark, df1)
    
    df2.write.mode("overwrite").parquet(f"pairs_simple")

if __name__ == "__main__":
    main()