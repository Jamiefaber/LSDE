from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType
from pyais import decode_msg
from geopy import distance
import sys
import numpy as np

def read(year, month, day):
    path = f"{year}/{month}/{day/}"





def main():
    spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "36000").getOrCreate()
    dfp = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferschema", "true").load("year/ports.csv*")
    
    dfp = dfp.select(col("latitude").alias("lat"), col("longitude").alias("lon"))
    broadcastedports = spark.sparkContext.broadcast(dfp.collect())


    for year in [2015, 2016, 2017]:
        for month in range(1, 13):
            for day in range(1, 32):
                for hour in range(0, 24):


    df1 = spark.read.option("sep","\t").csv(path+"12-45.txt")
    

    # df2 = spark.read.option("sep",",").csv(path+"12-46.txt")
    # df3 = spark.read.option("sep",",").csv(path+"12-47.txt")
    # df4 = spark.read.option("sep",",").csv(path+"12-48.txt")
    # df5 = spark.read.option("sep",",").csv(path+"12-49.txt")
    # df6 = spark.read.option("sep",",").csv(path+"12-50.txt")



if __name__ == "__main__":
    main()