import numpy as np
import matplotlib.pyplot as plt

# Set up modes and dirs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, unix_timestamp, when, mean
import pyspark.sql.functions as fc
from pyspark.sql.types import IntegerType

databricks = False
overwrite = False
is_yellow = False
yellow = "yellow" if is_yellow else "foil"
pick_up = "pickup"
drop_off = "dropoff"


if not databricks:
    data_dir = "/Users/kzmain/LSDE/data"
    spark = SparkSession.builder \
        .appName("Your App Name") \
        .config("spark.executor.memory", "14g") \
        .config("spark.driver.memory", "14g") \
        .getOrCreate()
else:
    data_dir = "/dbfs/mnt/group01"

df = spark.read.parquet(data_dir + "/foil/feature/*/*.gz.parquet")
df = df.select("trip_distance")\
    .withColumn("trip_distance", round(col("trip_distance"), 1))\
    .groupby("trip_distance")\
    .agg(count("trip_distance").alias("cnt"))\
    .sort("trip_distance")

x_axis_t = df.select("trip_distance").rdd.flatMap(lambda x: x).collect()
x_cunt_t = df.select("cnt").rdd.flatMap(lambda x: x).collect()

cnt = sum(x_cunt_t)

x_axis = []
x_cunt = []
for x in range(0, 100):
    x_axis.append(x_axis_t[x])
    x_cunt.append(x_cunt_t[x] / cnt * 100)

plt.bar(x_axis, x_cunt, color='#3977af')
plt.xlabel("Trip Distance")
plt.ylabel("Percentage")
plt.title("Trip Distance Distribution")

plt.xticks(x_axis, x_axis)
plt.xticks(rotation=30)

ax = plt.gca()
la = ax.get_xaxis().get_ticklabels()
print()
count = 0
for label in ax.get_xaxis().get_ticklabels():
    if count % 5 != 0:
        label.set_visible(False)
    else:
        label.set_visible(True)
    count = count + 1
plt.show()