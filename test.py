from pyspark import SparkContext as sc
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType, StringType, StructType, StructField, BooleanType

spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "3600000").getOrCreate()
pairs_simple = spark.read.format("parquet").option("header", "true").option("inferschema", "true").load("pairs_simple")
pairs_complex = spark.read.format("parquet").option("header", "true").option("inferschema", "true").load("pairs_complex")

print(pairs_simple.show())
print(pairs_complex.show())

print(pairs_simple.count())
print(pairs_complex.count())