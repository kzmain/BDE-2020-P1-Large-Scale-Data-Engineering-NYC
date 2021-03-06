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
df = df.filter(col("total_amount") > 0)\
    .filter(col("weather").isNotNull())\
    .select(col("total_amount"), col("weather")) \
    .groupby("weather") \
    .agg(fc.count(col("total_amount")).alias("cnt")) \
    .sort("cnt")

x_axis_t = df.select("weather").rdd.flatMap(lambda x: x).collect()
x_cunt_t = df.select("cnt").rdd.flatMap(lambda x: x).collect()

cnt = sum(x_cunt_t)

x_axis = []
x_cunt = []
x_axis_num = []
for x in range(0, len(x_cunt_t)):
    # if x_axis_t[x] < 0 or x_axis_t[x] > 50:
    #     continue
    x_axis.append(x_axis_t[x])
    x_axis_num.append(x)
    x_cunt.append(x_cunt_t[x] / cnt * 100)

import matplotlib.pyplot as plt
plt.rcParams["figure.figsize"] = (4.5 * 2.6, 4.5)

plt.bar(x_axis_num, x_cunt, color='#3977af', width=0.3)
plt.xticks(x_axis_num, x_axis)
plt.xlabel("Weather")
plt.ylabel("Trip Count Percentage")
plt.title("Weather influence on Trip Count")

plt.xticks(x_axis, x_axis)
plt.xticks(rotation=30)
ax = plt.gca()
la = ax.get_xaxis().get_ticklabels()

plt.show()
