from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, IntegerType, StringType, StructType, StructField, BooleanType
import numpy as np

def ports(spark, cell_width, cell_length):
    # Loads in csv file of all major port coordinates
#     dfp = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferschema", "true").load("/mnt/group05/ports.csv*")
#     dfp = dfp.select(col("latitude").alias("latp"), col("longitude").alias("longp"))
    
#     dfa = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferschema", "true").load("/mnt/group05/anchor.csv*")
#     dfa = dfa.select(col("latitude").alias("latp"), col("longitude").alias("longp"))
    
#     dfp = dfp.union(dfa)

#     port_map_schema = StructType([StructField("cell", FloatType(), True), \
#         StructField("Long_min", FloatType(), True), \
#         StructField("Long_max", FloatType(), True), \
#         StructField("Lat_min", FloatType(), True), \
#         StructField("Lat_max", FloatType(), True), \
#         ])
   
#     coords = []
#     cell_counter = 0
    neighbour_matrix = np.full((int(180/cell_length)+2, int(360/cell_width)), -1)
  
#     for i in np.arange(-90,90, cell_length):
#         for j in np.arange(-180,180,cell_width):
#             coords.append((float(cell_counter), float(j), float(j+cell_width), float(i), float(i+cell_length)))
#             cell_counter += 1

    cell_counter = 0
    for i in range(1, int(180/cell_length)+1):
        for j in range(int(360/cell_width)):
            neighbour_matrix[i, j] = float(cell_counter)
            cell_counter += 1

#     cell_map = spark.createDataFrame(data=coords, schema=port_map_schema)
#     broadcastedcells = spark.sparkContext.broadcast(cell_map.collect())
    
#     # make a list of neighbour tuples
    neighbour_tuples = []
    for i in range(1, neighbour_matrix.shape[0]-1):
        for j in range(neighbour_matrix.shape[1]):

            # to wrap around the longitude
            right = j+1
            if j == int(360/cell_width)-1:
                right = 0

            curr_id = float((neighbour_matrix[i,j]))
            neighbour_tuples.append((curr_id, curr_id))
            neighbour_id_1 = float(neighbour_matrix[i-1, j-1])
            neighbour_tuples.append((curr_id, neighbour_id_1))
            neighbour_id_2 = float(neighbour_matrix[i-1, j])
            neighbour_tuples.append((curr_id, neighbour_id_2))
            neighbour_id_3 = float(neighbour_matrix[i-1, right])
            neighbour_tuples.append((curr_id, neighbour_id_3))
            neighbour_id_4 = float(neighbour_matrix[i, j-1])
            neighbour_tuples.append((curr_id, neighbour_id_4))
            neighbour_id_5 = float(neighbour_matrix[i, right])
            neighbour_tuples.append((curr_id, neighbour_id_5))
            neighbour_id_6 = float(neighbour_matrix[i+1, j-1])
            neighbour_tuples.append((curr_id, neighbour_id_6))
            neighbour_id_7 = float(neighbour_matrix[i+1, j])
            neighbour_tuples.append((curr_id, neighbour_id_7))
            neighbour_id_8 = float(neighbour_matrix[i+1, right])
            neighbour_tuples.append((curr_id, neighbour_id_8))

    # make dataframe from neighbour tuples
    neighbour_schema = StructType([StructField("celln", FloatType(), True), \
        StructField("Neighbour", FloatType(), True), \
        ])

    dfn = spark.createDataFrame(data=neighbour_tuples, schema=neighbour_schema).where(col("Neighbour") != -1)

#     def groupToCell(arr):
#         lat, lon = arr[0], arr[1]
#         lat += 90
#         lon += 180
#         x_divs = 360 #360*1

#         cell = x_divs*int(lat)+int(lon)
#         return float(cell)

#     group_to_cell = udf(groupToCell, FloatType()).asNondeterministic()
    
#     dfp = dfp.select(col('latp'), col('longp'), group_to_cell(array(col('latp'), col('longp'))).alias('cellp')) \
# #         .orderBy('cellp')
    
#     dfp = dfp.groupBy("cellp").agg(collect_list(array(col("latp"), col("longp"))).alias("coords"))

    return dfn

def main():
    cell_width, cell_length = 1, 1
    # cell_width, cell_length = 0.1, 0.1
    spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "360000").getOrCreate()

    dfn = ports(spark, cell_width, cell_length)
    
#     cell_map.write.mode("overwrite").parquet(f"/mnt/group05/utils/cell_map")
    dfn.write.mode("overwrite").parquet(f"neighbours_ports")  
#     dfn.write.mode("overwrite").parquet(f"/mnt/group05/utils/ports")  

if __name__ == "__main__":
    main()