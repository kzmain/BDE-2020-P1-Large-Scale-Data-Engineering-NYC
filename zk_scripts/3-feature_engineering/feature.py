# %%

# Set up modes and dirs
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.functions import col

# %%
from pyspark.sql.types import IntegerType

overwrite = False
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
clean_file = clean_dbfs = (data_dir + "/{}".format(yellow) + "/cln/{}/{}.gz.parquet")
result_file = result_dbfs = (data_dir + "/{}".format(yellow) + "/feature/{}/{}.gz.parquet")

if databricks:
    clean_dbfs = clean_dbfs.replace("/dbfs", "")
    result_dbfs = result_dbfs.replace("/dbfs", "")

# %%

fr_year = 2009
fr_month = 1

to_year = 2017
to_month = 12


# %%
def check_file_exist(_path):
    if os.path.exists(_path) and not overwrite:
        print("[SYSTEM]: File exists: {}".format(_path))
        return True
    else:
        return False


# %%
def get_cln(_year, _month):
    return spark.read.parquet(clean_dbfs.format(_year, _month))


# %%
def feature_is_weekend(_in_df):
    _in_df = _in_df.withColumn("is_weekend", when(col("week_day") > 5, lit(True)).otherwise(lit(False)))
    return _in_df


def get_weather():
    # |         time_stamp|hour|day|high|low|baro|wind| wd|hum|weather|year|month|
    weather_dbfs = (data_dir + "/nyc/weather/parquet").replace("/dbfs", "")
    return spark.read.parquet(weather_dbfs).drop("time_stamp")


_weather_df = get_weather().cache()
_weather_df = _weather_df.withColumn("weather", when(
    (col("weather") == "light rain") |
    (col("weather") == "freezing rain") |
    (col("weather") == "heavy rain") |
    (col("weather") == "light freezing rain"),
    "rain").otherwise(col("weather")))

_weather_df = _weather_df.withColumn("weather", when(
    (col("weather") == "light snow") |
    (col("weather") == "heavy snow"),
    "snow").otherwise(col("weather")))

_weather_df = _weather_df.withColumn("weather", when(
    col("weather") == "ice fog",
    "fog").otherwise(col("weather")))

_weather_df = _weather_df.withColumn("weather", when(
    (col("weather") == "clouds") |
    (col("weather") == "low clouds"),
    "cloudy").otherwise(col("weather")))

_weather_df = _weather_df.withColumn("weather", when(
    (col("weather") == "haze"),
    "overcast").otherwise(col("weather")))

_weather_df = _weather_df.withColumn("weather", when(
    (col("weather") == "clear") |
    (col("weather") == "cool") |
    (col("weather") == "chilly") |
    (col("weather") == "frigid") |
    (col("weather") == "quite cool") |
    (col("weather") == "hot") |
    (col("weather") == "warm") |
    (col("weather") == "mild") |
    (col("weather") == "refreshingly cool") |
    (col("weather") == "pleasantly warm") |
    (col("weather") == "mild") |
    (col("weather") == "cold")
    , "sunny").otherwise(col("weather")))


# %%
def feature_weather(_in, _year):
    _wh = _weather_df.filter(col("year") == _year)
    _pm = "pick_month"
    _pd = "pick_day"
    _ph = "pick_hour"
    _m = "month"
    _d = "day"
    _h = "hour"
    _l = "left_outer"

    return _in.join(_wh, (_in[_pm] == _wh[_m]) & (_in[_pd] == _wh[_d]) & (_in[_ph] == _wh[_h]), how=_l)


# %%
dur_col = "duration_second"

drop_stmp = "dropoff_datetime"
pick_stmp = "pickup_datetime"
prev_stmp = "predrop_datetime"

trip_cruise = "trip_cruise_second"

day_is_sleep = "day_is_sleep_in_day"
day_sleep_len = "day_sleep_second"
day_trip_sum = "day_trip_count"
day_work_len = "day_work_second"

mean_cruise = "day_mean_cruise_second_per_trip"
mean_travel = "day_mean_travel_second_per_trip"
mean_income = "day_mean_income_per_trip"
mean_tip = "day_mean_tip_per_trip"

month_income = "month_income_total"
month_work_len = "month_work_hour"
month_weekend = "month_work_on_weekend"

# mean_month_cruise = "month_mean_cruise_second_per_trip"
# mean_month_travel = "month_mean_travel_second_per_trip"
mean_month_income = "month_income_per_hour"
# mean_month_tip = ""

_w1 = Window.partitionBy("medallion", "hack_license", "pick_month", "pick_day").orderBy(pick_stmp)
_w2 = Window.partitionBy("medallion", "hack_license", "pick_month", "pick_day")
month_window = Window.partitionBy("medallion", "hack_license", "pick_month")


# %%
def feature_cruise_len(_in_df):
    _in_df = _in_df.withColumn(prev_stmp, lag(_in_df[drop_stmp]).over(_w1))
    _in_df = _in_df.withColumn(trip_cruise, unix_timestamp(col(pick_stmp)) - unix_timestamp(col(prev_stmp)))
    _in_df = _in_df.withColumn(trip_cruise, when(col(trip_cruise) > 4 * 60 * 60, lit(None)).otherwise(col(trip_cruise)))
    # The datetime log error by the machine can be -1
    _in_df = _in_df.filter(col(trip_cruise) > 0)
    return _in_df


# %%
def feature_sleep_in_day(_in_df):
    _in_df = _in_df.withColumn(day_is_sleep, when(col(trip_cruise) > 4 * 60 * 60, True).otherwise(False))
    _in_df = _in_df.withColumn(day_is_sleep, max(col(day_is_sleep)).over(_w2))
    _in_df = _in_df.withColumn(day_sleep_len, when(col(trip_cruise) > 4 * 60 * 60, col(trip_cruise)).otherwise(lit(0)))
    return _in_df


# %%
def feature_trip_count(_in_df):
    _in_df = _in_df.withColumn(day_trip_sum, count(col(pick_stmp)).over(_w2))
    return _in_df


# %%
def feature_work_time(_in_df):
    _srt = "day_start_stamp"
    _end = "day_end_stamp"
    _slp = "sleep"
    _in_df = _in_df.withColumn(_srt, min(unix_timestamp(col(pick_stmp))).over(_w2))
    _in_df = _in_df.withColumn(_end, max(unix_timestamp(col(drop_stmp))).over(_w2))
    _in_df = _in_df.withColumn(_slp, sum(col(day_sleep_len)).over(_w2))
    _in_df = _in_df.withColumn(day_work_len, col(_end) - col(_srt) - col(_slp))
    return _in_df


# %%
def feature_foil(_in_df):
    # Get trip_cruise
    _in_df = feature_cruise_len(_in_df)
    _in_df = feature_sleep_in_day(_in_df)
    _in_df = feature_work_time(_in_df)
    _in_df = feature_trip_count(_in_df)

    _in_df = _in_df \
        .withColumn(mean_travel, mean(dur_col).over(_w2)) \
        .withColumn(mean_cruise, mean(trip_cruise).over(_w2)) \
        .withColumn(mean_tip, mean("tip_amount").over(_w2)) \
        .withColumn(mean_income, mean("total_amount").over(_w2))
    _m_df = _in_df \
        .select(["medallion", "hack_license", "pick_month", "pick_day", day_work_len])
    _m_df = _m_df.drop_duplicates()
    _m_df = _m_df.groupby("medallion", "hack_license", "pick_month") \
        .agg(sum(day_work_len).alias(month_work_len)) \
        .withColumn(month_work_len, (col(month_work_len) / lit(60 * 60)).cast(IntegerType()))

    _in_df = _in_df.join(_m_df, ["medallion", "hack_license", "pick_month"])
    _in_df = _in_df \
        .withColumn(month_income, sum("total_amount").over(month_window)) \
        .withColumn(mean_month_income, (col(month_income) / col(month_work_len))) \
        .withColumn(month_weekend, max('is_weekend').over(month_window))
    return _in_df


# %%

# %%

for year in range(2009, 2020):
    for month in range(1, 13):
        if not os.path.exists(clean_file.format(year, month)):
            continue
        if check_file_exist(result_file.format(year, month)):
            print("HAVE : {}-{}".format(year, month))
            continue
        print("Start: {}-{}".format(year, month))
        print("[System]: Get cleaned parquet")
        cln = get_cln(year, month)
        print("[System]: Feature is weekend")
        cln = feature_is_weekend(cln)
        print("[System]: Feature weather")
        cln = feature_weather(cln, year)

        if not is_yellow:
            print("[System]: Feature FOIL")
            cln = feature_foil(cln)

        print("[System]: WRITE OUT FILE")
        cln.write.mode("overwrite").option("compression", "gzip") \
            .parquet(result_dbfs.format(year, month))
