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

df = spark.read.parquet(data_dir + "/foil/feature/*/*.gz.parquet")

# trip based average distance
df = df.filter(col("hour") > 0)\
    .select(col("total_amount"), col("hour")) \
    .groupby("hour") \
    .agg(fc.count(col("total_amount")).alias("cnt")) \
    .sort("hour")

x_axis_t = df.select("hour").rdd.flatMap(lambda x: x).collect()
x_cunt_t = df.select("cnt").rdd.flatMap(lambda x: x).collect()

cnt = sum(x_cunt_t)

x_axis = []
x_cunt = []
for x in range(0, len(x_cunt_t)):
    if x_axis_t[x] < 0 or x_axis_t[x] > 50:
        continue
    x_axis.append(x_axis_t[x])
    x_cunt.append(x_cunt_t[x] / cnt * 100)

import matplotlib.pyplot as plt
plt.rcParams["figure.figsize"] = (4.5 * 2.6, 4.5)

plt.bar(x_axis, x_cunt, color='#3977af', width=0.3)
plt.xlabel("Trip Pick-up Hour")
plt.ylabel("Trip Count Percentage")
plt.title("Trip Pick-up Hour influence on Trip Count")

# plt.ylim(8, 20)

plt.xticks(x_axis, x_axis)
plt.xticks(rotation=30)
ax = plt.gca()
la = ax.get_xaxis().get_ticklabels()
count = 0

plt.show()
