#%%

import os
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, DoubleType

#%%

# Set up modes and dirs
overwrite  = False
databricks = False
if not databricks:
    from util import folder
    data_dir = folder.DATA_DIR
    from_dir = folder.YELL_DIR
    spark = SparkSession.builder.getOrCreate()
else:
    data_dir = "/dbfs/mnt/group01"
    from_dir = "/dbfs/mnt/nyc-tlc/trip data"

to_file = to_dbfs = data_dir + "/yellow/raw/{}/{}.gz.parquet"
fr_file = fr_dbfs = from_dir + "/yellow_tripdata_{}-{}.csv"

if databricks:
    to_dbfs = to_dbfs.replace("/dbfs", "")
    fr_dbfs = fr_dbfs.replace("/dbfs", "")
dirs = [data_dir]

#%%

for d in dirs:
    if not os.path.exists(d):
        os.makedirs(d)

#%%

year_start = 2009
year_end   = 2016
year_range = range(year_start, year_end + 1)

month_range = ["01", "02", "03", "04", "05", "06",
               "07", "08", "09", "10", "11", "12"]

#%%

def check_file_exist(_path):
    if os.path.exists(_path) and not overwrite:
            print("[SYSTEM]: File exists: {}".format(_path))
            return True
    else:
        return False

#%%

def update_columns(_in_df):
    _old_columns = _in_df.columns
    _new_columns = [_col.lower()
               .strip()
               .replace("vendor_name", "vendor_id")
               .replace("vendorid", "vendor_id")
               .replace("start_lon", "pickup_longitude")
               .replace("start_lat", "pickup_latitude")
               .replace("end_lon", "dropoff_longitude")
               .replace("end_lat", "dropoff_latitude")
               .replace("amt", "amount")
               .replace("trip_pickup_datetime" , "pickup_datetime")
               .replace("trip_dropoff_datetime", "dropoff_datetime")
               .replace("fwd_flag", "forward")
               .replace("tpep_", "")
               .replace("ratecodeid", "rate_code")
               .replace("improvement_", "")
           for _col in _old_columns]
    for _cnt in range(0, len(_old_columns)):
        _in_df = _in_df.withColumnRenamed(_old_columns[_cnt], _new_columns[_cnt])
        if "date" in _new_columns[_cnt]:
            _in_df = _in_df.withColumn(_new_columns[_cnt], col(_new_columns[_cnt]).cast(TimestampType()))
        else:
            _in_df = _in_df.withColumn(_new_columns[_cnt], col(_new_columns[_cnt]).cast(DoubleType()))
    return _in_df

#%%

def select_columns(_in_df):
    return _in_df.select(["pickup_datetime", "dropoff_datetime",
                 "dropoff_latitude", "dropoff_longitude",
                 "pickup_latitude", "pickup_longitude",
                "trip_distance", "tip_amount", "total_amount"])

#%%

def csv_parquet():
    info_title = lambda _y, _m: print("____________________________YELLOW_{}_{}____________________________".format(_y, _m))
    info_start = lambda _y, _m: print("[SYSTEM]: Start  {}-{}".format(_y, _m))
    info_end   = lambda _y, _m: print("[SYSTEM]: Finish {}-{}".format(_y, _m))

    for _year in year_range:
        for _month in month_range:
            if _year == 2016 and int(_month) > 6:
                break
            _dest = to_dbfs.format(_year, int(_month))
            _from = fr_dbfs.format(_year, _month)
            _file = to_file.format(_year, int(_month))
            if not os.path.exists(fr_file.format(_year, _month)):
                continue
            info_title(_year, _month)
            if check_file_exist(_file):
                continue
            info_start(_year, _month)
            _df = spark.read.option("header", True).csv(_from)
            _df = update_columns(_df)
            # _df = select_columns(_df)
            _df.write.mode("overwrite")\
                .option("compression", "gzip")\
                .parquet(_dest)
            info_end(_year, _month)

#%%

csv_parquet()

#%%

# df = spark.read.option("header", True).csv("/Users/kzmain/LSDE/data/yellow_download/yellow_tripdata_2009-01.csv")
# df.show()

#%%

# df.printSchema()

#%%

# root-foil
#  |-- medallion: integer (nullable = true)
#  |-- hack_license: integer (nullable = true)
#  |-- pickup_datetime: timestamp (nullable = true)
#  |-- dropoff_latitude: double (nullable = true)
#  |-- dropoff_longitude: double (nullable = true)
#  |-- pickup_latitude: double (nullable = true)
#  |-- pickup_longitude: double (nullable = true)
#  |-- trip_distance: double (nullable = true)
#  |-- trip_time_in_secs: integer (nullable = true)
#  |-- dropoff_datetime: timestamp (nullable = true)
#  |-- rate_code: short (nullable = true)
#  |-- tip_amount: double (nullable = true)
#  |-- total_amount: double (nullable = true)
