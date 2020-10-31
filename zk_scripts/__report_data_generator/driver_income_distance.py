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

# driver distance vs driver income
df = df.select(col("hack_license"), col("month_income_total"), col("trip_distance")) \
    .groupby("hack_license", "month_income_total") \
    .agg(fc.mean(col("trip_distance")).alias("mean_distance")) \
    .groupby("hack_license") \
    .agg(mean(col("month_income_total")).alias("mean_amount"), mean(col("mean_distance")).alias("mean_distance"))

df = df.withColumn("mean_distance", col("mean_distance") / 0.5) \
    .withColumn("mean_distance", col("mean_distance").cast(IntegerType())) \
    .withColumn("mean_distance", col("mean_distance") * 0.5) \
    .groupby("mean_distance") \
    .agg(mean(col("mean_amount")).alias("mean_amount")) \
    .sort("mean_distance")

x_axis_t = df.select("mean_distance").rdd.flatMap(lambda x: x).collect()
x_cunt_t = df.select("mean_amount").rdd.flatMap(lambda x: x).collect()


x_axis = []
x_cunt = []
for x in range(0, len(x_cunt_t)):
    if x_axis_t[x] < 0 or x_axis_t[x] > 21:
        continue
    x_axis.append(x_axis_t[x])
    x_cunt.append(x_cunt_t[x])

import matplotlib.pyplot as plt
plt.rcParams["figure.figsize"] = (4.5 * 2.6, 4.5)

plt.bar(x_axis, x_cunt, color='#3977af', width=0.3)
plt.xlabel("Driver's Mean Trip Distance")
plt.ylabel("Driver's Mean Income")
plt.title("Driver's Mean Trip Distance influence on Driver's Income")

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