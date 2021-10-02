from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import pyais

spark = SparkSession.builder.getOrCreate()

df1 = spark.read.option("sep",",").csv("/mnt/lsde/ais/2015/01/06/12-45.txt")
df2 = spark.read.option("sep",",").csv("/mnt/lsde/ais/2015/01/06/12-46.txt")
df3 = spark.read.option("sep",",").csv("/mnt/lsde/ais/2015/01/06/12-47.txt")
df4 = spark.read.option("sep",",").csv("/mnt/lsde/ais/2015/01/06/12-48.txt")
df5 = spark.read.option("sep",",").csv("/mnt/lsde/ais/2015/01/06/12-49.txt")
df6 = spark.read.option("sep",",").csv("/mnt/lsde/ais/2015/01/06/12-50.txt")

def decode(filename):
    for msg in FileReaderStream(filename):
        decoded_message = msg.decode()
        ais_content = decoded_message.content
        if ais_content['type'] in [1,2,3,5]:
            print(ais_content['GOS'])

decode(df1) 
