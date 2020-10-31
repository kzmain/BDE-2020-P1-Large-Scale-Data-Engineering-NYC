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

# Duration
duration_delta = "duration"
duration_secod = "duration_second"
df = df.withColumn(duration_secod, unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime")))


df = df.select("duration_second")\
    .withColumn("duration_second", when(col("duration_second") < 0, 0).otherwise(col("duration_second")))

df = df.withColumn("duration_second", col("duration_second") / 15)\
    .withColumn("duration_second", col("duration_second").cast(IntegerType()))\
    .withColumn("duration_second", col("duration_second") * 15)\
    .groupby("duration_second")\
    .agg(count("duration_second").alias("cnt"))\
    .sort("duration_second")

x_axis_t = df.select("duration_second").rdd.flatMap(lambda x: x).collect()
x_cunt_t = df.select("cnt").rdd.flatMap(lambda x: x).collect()

cnt = sum(x_cunt_t)

x_axis = []
x_cunt = []
for x in range(0, 30):
    x_axis.append(x_axis_t[x])
    x_cunt.append(x_cunt_t[x] / cnt * 100)

import matplotlib.pyplot as plt
plt.rcParams["figure.figsize"] = (4.5 * 2.6, 4.5)

plt.bar(x_axis, x_cunt, color='#3977af', width=10)
plt.xlabel("Trip Duration (Second)")
plt.ylabel("Percentage")
plt.title("Trip Duration Distribution")

plt.xticks(x_axis, x_axis)
plt.xticks(rotation=30)
ax = plt.gca()
la = ax.get_xaxis().get_ticklabels()
count = 0
for label in ax.get_xaxis().get_ticklabels():
    if count % 4 != 0:
        label.set_visible(False)
    else:
        label.set_visible(True)
    count = count + 1
plt.show()






