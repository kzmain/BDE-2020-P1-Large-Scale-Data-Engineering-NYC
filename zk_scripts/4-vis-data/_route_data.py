# %%

# Set up modes and dirs
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.functions import col

# %%
from pyspark.sql.types import IntegerType

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
rt_file = rt_dbfs = (data_dir + "/iv/route_new.gz.parquet")

if databricks:
    in_dbfs = in_dbfs.replace("/dbfs", "")
    rt_dbfs = rt_dbfs.replace("/dbfs", "")

df = spark.read.parquet(in_dbfs)

pick_step = 15
temp_step = 4

df = df.withColumn("pick_min", col("pick_min") - col("pick_min") % pick_step)
df = df.withColumn("low", col("low") - col("low") % temp_step)

zone_id = "zone_id"
yr = "year"
mt = "minute"
hr = "hour"
tp = "temperature"
sn = "season"
wt = "weather"

def get_rt_df(_in):
    _pyear = "pick_year"
    _pmonth = "pick_month"
    _phour = "pick_hour"
    _pday = "pick_day"
    _pminute = "pick_min"
    _ptemperature = "low"
    _u = "u"
    _v = "v"

    cu = col("pickup_u").alias(_u)
    cv = col("pickup_v").alias(_v)
    cmt = col(_pminute).alias(mt)
    chr = col(_phour).alias(hr)
    ctp = col(_ptemperature).alias(tp)

    _route_df = _in.select(cu, cv, cmt, chr, ctp, col(sn), col(wt), "trip_distance", "duration_second", "tip_amount", "total_amount", "trip_distance")

    _route_df = _route_df.groupby(_u, _v, mt, hr, tp, sn, wt) \
        .agg(
        mean("trip_distance").alias("mean_distance"),
        mean("duration_second").alias("mean_second"),
        mean("tip_amount").alias("mean_tip"),
        mean("total_amount").alias("mean_amount"),
        count("trip_distance").alias("trip_count")
    )
    # year, month, day, hour, minute, season, temperature, weather
    _days_df = _in.select(col(_pyear), col(_pmonth), col(_pday), col(_phour).alias(hr), col(_pminute).alias(mt), col(sn),
                          col(_ptemperature).alias(tp), col(wt)).drop_duplicates()

    # get condition days
    _days_df = _days_df.groupby(col(mt), col(hr), col(tp), col(sn), col(wt))\
        .agg(countDistinct(col(_pyear), col(_pmonth), col(_pday)).alias("condition_days"))

    # get all days
    _route_df = _route_df.join(_days_df, on=[hr, mt, sn, tp, wt], how="left_outer")\
        .withColumn("mean_trip", col("trip_count") / col("condition_days"))

    return _route_df


df = get_rt_df(df)
df.coalesce(1).write.mode("overwrite").option("compression", "gzip").partitionBy([mt, hr, sn, wt, tp]).parquet(rt_dbfs)
