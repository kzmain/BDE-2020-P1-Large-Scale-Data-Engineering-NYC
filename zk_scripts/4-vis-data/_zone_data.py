import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col

# %%

overwrite = True
databricks = False
is_yellow = False

yellow = "yellow" if is_yellow else "foil"
pick_up = "pickup"
drop_off = "dropoff"

# %%

if not databricks:
    data_dir = "/Users/kzmain/LSDE/data"
    spark = SparkSession.builder \
        .appName("Your App Name") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
else:
    data_dir = "/dbfs/mnt/group01"
in_file = in_dbfs = (data_dir + "/{}".format(yellow) + "/feature/*/*.gz.parquet")
zn_file = zn_dbfs = (data_dir + "/iv/zone_new.gz.parquet")

if databricks:
    in_dbfs = in_dbfs.replace("/dbfs", "")
    zn_dbfs = zn_dbfs.replace("/dbfs", "")

df = spark.read.parquet(in_dbfs)

pick_step = 15
temp_step = 4

df = df.withColumn("pick_min", col("pick_min") - col("pick_min") % pick_step)
df = df.withColumn("low", col("low") - col("low") % temp_step)

# %%
zone_id = "zone_id"
yr = "year"
mt = "minute"
hr = "hour"
tp = "temperature"
sn = "season"
wt = "weather"

czone_id = col("pickup_zone_id").alias(zone_id)
cpick_mt = col("pick_min").alias(mt)
cpick_hr = col("pick_hour").alias(hr)
cpick_yr = col("pick_year").alias(yr)
cpick_tp = col("low").alias(tp)
cpick_sn = col(sn)
cpick_wt = col(wt)
# year
df = df.select(czone_id, cpick_mt, cpick_hr, cpick_yr, cpick_tp, cpick_sn, cpick_wt, "total_amount", "tip_amount", "duration_second", "trip_distance")

years = [x["year"] for x in df.select("year").drop_duplicates().collect()]

final_df = df.groupby(zone_id, mt, hr, tp, sn, wt).agg(
    count(zone_id).alias("trip_count"),
    mean("total_amount").alias("mean_amount"),
    mean("tip_amount").alias("mean_tip"),
    mean("duration_second").alias("mean_duration"),
    mean("trip_distance").alias("mean_distance"),
)

final_df = final_df.withColumn("mean_trip_cnt", col("trip_count")/lit(len(years)))

for year in years:
    ydf = df.filter(col("year") == year).groupby(zone_id, mt, hr, tp, sn, wt).agg(
        count(zone_id).alias("{}_trip_count".format(year)),
    )
    final_df = final_df.join(ydf, [zone_id, mt, hr, tp, sn, wt], how="left_outer")
    final_df = final_df.fillna(0, subset=["{}_trip_count".format(year)])

final_df.coalesce(1).write.mode("overwrite").option("compression", "gzip").partitionBy([mt, hr, sn, wt, tp]).parquet(zn_dbfs)
