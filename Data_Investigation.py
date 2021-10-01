# Databricks notebook source
# shows txt files for the first year/month/day
dbutils.fs.ls("/mnt/lsde/ais/2015/01/06")

# COMMAND ----------

df1 = spark.read.option("sep",",").csv("/mnt/lsde/ais/2015/01/06/12-46.txt")
df1.show()
# _c0 = indicates format (AIVDM in our case)
# _c1 = count fragments accumulating  message
# _c2 = fragment number this sentence
# _c3 = sequential message ID for multi-sentence messages
# _c4 = radio channel code
# _c5 = data payload
# _c6 = number of fill bits for padding

# COMMAND ----------

# amount rows in 12-46.txt
df1.count()

# COMMAND ----------

# size of 12-46: 4389593 or 4.4mb
dbutils.fs.ls("/mnt/lsde/ais/2015/01/06/12-46.txt")

# COMMAND ----------

# amount of total txt files
file_counter = 0
day_counter = 0
month_counter = 0
for year in [year.path for year in dbutils.fs.ls("/mnt/lsde/ais")]:
  MonthFolderlist = [month.path for month in dbutils.fs.ls(year)]
  for month in MonthFolderlist:
    DayFolderlist = [day.path for day in dbutils.fs.ls(month)]
    month_counter += 1
    for day in DayFolderlist:
      day_counter += 1
#       for file in dbutils.fs.ls(day): # commented these lines because takes 5 min to count all files
#         file_counter += 1

print(file_counter)
print(day_counter)
print(month_counter)
# so 825694 files in 600 days accross 29 months in 3 years


# COMMAND ----------

# installs AIS decoder
%pip install pyais

# COMMAND ----------

from pyais import FileReaderStream

# chose whice file to investigate here:
filename = "/dbfs/mnt/lsde/ais/2015/01/06/12-46.txt"
boatslist=[]
message_counter = 0

# counts unique amount ships
for msg in FileReaderStream(filename):
    decoded_message = msg.decode()
    ais_content = decoded_message.content
    if ais_content:
        message_counter += 1
        if ais_content['mmsi'] not in boatslist:
            boatslist.append(ais_content['mmsi'])
print(len(boatslist))
print(message_counter)
# Base Station Report example https://gpsd.gitlab.io/gpsd/AIVDM.html#_types_1_2_and_3_position_report_class_a
# {'type': 4, 'repeat': 3, 'mmsi': '003669962', 'year': 2015, 
# 'month': 1, 'day': 5, 'hour': 10, 'minute': 58, 'second': 50, 
# 'accuracy': 1, 'lon': -76.00333333333333, 'lat': 44.255833333333335, 
# 'epfd': <EpfdType.Surveyed: 7>, 'raim': 0, 'radio': 114699}

# COMMAND ----------

# calculates percentage different type messages
# message types other than 1, 3, 4, 5, 18, and 24 are unusual or rare
counter = 0
type1=type2=type3=type5= 0
for msg in FileReaderStream(filename):
    decoded_message = msg.decode()
    ais_content = decoded_message.content
    if ais_content:
        counter += 1
        if ais_content['type'] == 1:
            type1 += 1
        if ais_content['type']  == 2:
            type2 += 1
        if ais_content['type']  == 3:
            type3 += 1
        if ais_content['type']  == 5:
            type5 += 1
      
type1P = type1/counter*100
type2P = type2/counter*100
type3P = type3/counter*100
type5P = type5/counter*100
Percent = [type1P,type2P,type3P,type5P]
Type = ['1','2','3','5']

print("Total percentage relevant type:",int(type1P+type2P+type3P+type5P),"%" )

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

# plots message type percentages
fig, ax = plt.subplots()
ax.set_xlabel("Type")
ax.set_ylabel("Frequency")
ax.bar(Type,Percent)
ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=100))
ax.set_ylim([0,100])
plt.show()

# COMMAND ----------

# Empty messages 
empty_list = []
valid_message_counter = 0 
message_counter = 0
types = list(range(1,28))
typeless = []
for msg in FileReaderStream(filename):
    message_counter += 1
    decoded_message = msg.decode()
    ais_content = decoded_message.content
    if not ais_content:
        empty_list.append(ais_content)
    else:
        valid_message_counter += 1
        if ais_content['type'] not in types:
          typeless.append(ais_content)
          
print(len(empty_list))
print(len(typeless))
print(message_counter)
print('number valid:',valid_message_counter-len(typeless))


          
