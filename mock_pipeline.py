from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType
from pyais import decode_msg

def main ():

    spark = SparkSession.builder.getOrCreate()

    path = "data/"

    df1 = spark.read.option("sep","\t").csv(path+"12-45.txt")
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
    # print(df1.show(n=5))
    print(df2.show(n=5))

if __name__ == "__main__":
    main()