import matplotlib.pyplot as plt
import pyspark.sql.functions as fc
# Set up modes and dirs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean
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

zone_df = spark.read.csv(data_dir + "/taxi_zones.csv", header=True, inferSchema=True)\
    .withColumn("zone", fc.regexp_replace(col("zone"), "( )City$|( )North$|( )West$|( )East$|( )South$", ""))\
    .select("zone", "LocationID")\
    .withColumnRenamed("LocationID", "pickup_zone_id")
zone_df.show(100, truncate=False)

df = spark.read.parquet(data_dir + "/foil/feature/*/*.gz.parquet")

df = df.filter((col("hour") == 5) & (col("weather") == "snow") & ((col("low") < -4) | (col("low") > 29)))
# df = df.groupby("pickup_zone_id")\
#     .agg(fc.count(col("total_amount")).alias("cnt"))\
#     .withColumnRenamed("pickup_zone_id", "zone_id")\
#     .sort(col("cnt"), ascending=False)

df = df\
    .join(zone_df, "pickup_zone_id").groupby("zone")\
    .agg(fc.count(col("total_amount")).alias("cnt"))\
    .sort(col("cnt"),ascending=False)

x_axis_t = df.select("zone").rdd.flatMap(lambda x: x).collect()
x_cunt_t = df.select("cnt").rdd.flatMap(lambda x: x).collect()

print(x_axis_t)
print(x_cunt_t)

x_axis = []
x_axis_num = []
x_cunt = []
cnt = 0
for x in range(0, len(x_cunt_t)):
    if x_cunt_t[x] < 100:
        continue
    x_axis_num.append(x)
    x_axis.append(x_axis_t[x])
    x_cunt.append(x_cunt_t[x])
    cnt = cnt + 1


import matplotlib.pyplot as plt
plt.rcParams["figure.figsize"] = (4.5 * 2.6, 4.5)

plt.bar(x_axis_num, x_cunt, color='#3977af', width=0.3)
plt.xlabel("Zone")
plt.ylabel("Count")
plt.title("Trip Count of Zone")
plt.xticks(x_axis_num, x_axis)
plt.xticks(rotation=90)
plt.show()
