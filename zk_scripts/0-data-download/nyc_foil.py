#%%

import glob
import json
import os
import urllib
import requests
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType, DoubleType, ShortType

#%%

# Set up modes and dirs
overwrite  = False
databricks = False
if not databricks:
    data_dir = "/Users/kzmain/LSDE/data"
    spark = SparkSession.builder.getOrCreate()
else:
    data_dir = "/dbfs/mnt/group01"

foil_dir = foil_dbfs = os.path.join(data_dir, "foil")
down_dir = down_dbfs = os.path.join(foil_dir, "down")
csvs_dir = csvs_dbfs = os.path.join(foil_dir, "csv")
zips_dir = zips_dbfs = os.path.join(foil_dir, "zip")
raws_dir = raws_dbfs = os.path.join(foil_dir, "raw")

if databricks:
    foil_dbfs = foil_dbfs.replace("/dbfs", "")
    down_dbfs = down_dbfs.replace("/dbfs", "")
    csvs_dbfs = csvs_dbfs.replace("/dbfs", "")
    zips_dbfs = zips_dbfs.replace("/dbfs", "")
    raws_dbfs = raws_dbfs.replace("/dbfs", "")
dirs = [data_dir, foil_dir, down_dir, csvs_dir, zips_dir]

#%%

for d in dirs:
    if not os.path.exists(d):
        os.makedirs(d)

#%%

year_start = 2010
year_end   = 2013
moth_start = 1
moth_end   = 12

#%%

def check_file_exist(_path):
    if os.path.exists(_path) and not overwrite:
            print("[SYSTEM]: File exists: {}".format(_path))
            return True
    else:
        return False

#%%

def download_foil_data():
    info_title = lambda _file_name : print("____________________________FOIL_DOWNLOAD_{}____________________________".format(_file_name))
    info_start = lambda _file_name : print("[SYSTEM]: Start  {}".format(_file_name))
    info_end   = lambda _file_name : print("[SYSTEM]: Finish {}".format(_file_name))

    # Start to download the FOIL files
    data_page = "https://databank.illinois.edu/datasets/IDB-9610843"
    down_page = "https://databank.illinois.edu/datafiles/{}/download"

    requ = requests.get(data_page)
    resp = json.loads(requ.text)

    for datafile in resp['datafiles']:
        # databricks storage location
        # remote download location
        local_path = os.path.join(down_dir, datafile["binary_name"])
        remot_path = down_page.format(datafile["web_id"])

        info_title(datafile["binary_name"])
        if check_file_exist(local_path):
            continue
        # start to download the FOIL data
        info_start(local_path)
        urllib.request.urlretrieve(remot_path, local_path)
        info_end(local_path)

#%%

download_foil_data()

#%%

def extract_foil_down():
    info_title = lambda _file_name : print("____________________________FOIL_EXTRACT_{}____________________________".format(_file_name))
    info_start = lambda _file_name : print("[SYSTEM]: Start  {}".format(_file_name))
    info_end   = lambda _file_name : print("[SYSTEM]: Finish {}".format(_file_name))
    zip_files = glob.glob(os.path.join(down_dir, "*.zip"))
    # zip_files = glob.glob(os.path.join(down_dir, "FOIL2011.zip"))
    for zip_file in zip_files:
        target_folder = os.path.join(zips_dir, zip_file.replace(down_dir + "/", "").replace(".zip", ""))
        if check_file_exist(target_folder):
            continue
        info_title(zip_file.replace(down_dir + "/", ""))
        command = "cd {} && jar -xvf {}".format(zips_dir, zip_file)
        info_start(zip_file.replace(down_dir + "/", ""))
        os.system(command)
        info_end(zip_file.replace(down_dir + "/", ""))

#%%

extract_foil_down()

#%%

def extract_foil_zip():
    info_title = lambda _file_name : print("____________________________FOIL_ZIP_EXTRACT_{}____________________________".format(_file_name))
    info_start = lambda _file_name : print("[SYSTEM]: Start {}".format(_file_name))
    info_end   = lambda _file_name : print("[SYSTEM]: Finish {}".format(_file_name))

    for _year in range(year_start, year_end + 1):
        zip_foil = os.path.join(zips_dir, "FOIL{}".format(_year))
        zip_files = glob.glob(os.path.join(zip_foil, "*.zip"))
        tgt_foil = os.path.join(csvs_dir, "{}".format(_year))

        info_title(_year)
        for zip_file in zip_files:
            if not os.path.exists(tgt_foil):
                os.makedirs(tgt_foil)

            command = "cd {} && jar -xvf {}".format(tgt_foil, zip_file)
            tar_file = zip_file\
                .replace(zips_dir + "/", "")\
                .replace("FOIL{}/".format(_year), "")
            if check_file_exist(os.path.join(tgt_foil, tar_file).replace("zip", "csv")):
                continue
            info_start(tar_file)
            os.system(command)
            info_end(tar_file)

#%%

extract_foil_zip()

#%%

def process_trip(_in_df):
    return _in_df\
        .withColumn("medallion", col("medallion").cast(IntegerType()))\
        .withColumn("hack_license", col(" hack_license").cast(IntegerType()))\
        .drop(" hack_license")\
        .withColumn("pickup_datetime", col(" pickup_datetime").cast(TimestampType()))\
        .drop(" pickup_datetime")\
        .withColumn("dropoff_latitude", col(" dropoff_latitude").cast(DoubleType()))\
        .drop(" dropoff_latitude")\
        .withColumn("dropoff_longitude", col(" dropoff_longitude").cast(DoubleType()))\
        .drop(" dropoff_longitude")\
        .withColumn("pickup_latitude", col(" pickup_latitude").cast(DoubleType()))\
        .drop(" pickup_latitude")\
        .withColumn("pickup_longitude", col(" pickup_longitude").cast(DoubleType()))\
        .drop(" pickup_longitude")\
        .withColumn("trip_distance", col(" trip_distance").cast(DoubleType()))\
        .drop(" trip_distance")\
        .withColumn("trip_time_in_secs", col(" trip_time_in_secs").cast(IntegerType()))\
        .drop(" trip_time_in_secs")\
        .withColumn("dropoff_datetime", col(" dropoff_datetime").cast(TimestampType()))\
        .drop(" dropoff_datetime")\
        .withColumn("rate_code", col(" rate_code").cast(ShortType()))\
        .drop(" rate_code")\
        .drop(" passenger_count")\
        .drop(" vendor_id")\
        .drop(" store_and_fwd_flag")

def process_fare(_in_df):
    return _in_df.withColumn("medallion", col("medallion").cast(IntegerType()))\
    .withColumn("hack_license", col(" hack_license").cast(IntegerType()))\
    .drop(" hack_license")\
    .withColumn("pickup_datetime", col(" pickup_datetime").cast(TimestampType()))\
    .drop(" pickup_datetime")\
    .withColumn("tip_amount", col(" tip_amount").cast(DoubleType()))\
    .drop(" tip_amount")\
    .withColumn("total_amount", col(" total_amount").cast(DoubleType()))\
    .drop(" total_amount")\
    .drop(" vendor_id")\
    .drop(" payment_type")\
    .drop(" surcharge")\
    .drop(" mta_tax")\
    .drop(" tolls_amount")\
    .drop(" fare_amount")

def combine_raw_data():
    info_title = lambda _y, _m : print("____________________________FOIL_COMBINE_{}_{}____________________________".format(_y, _m))
    info_start = lambda _y, _m : print("[SYSTEM]: Start  {}-{}".format(_y, _m))
    info_end   = lambda _y, _m : print("[SYSTEM]: Finish {}-{}".format(_y, _m))
    for _year in range(year_start, year_end + 1):
        for _month in range(moth_start, moth_end + 1):
            info_title(_year, _month)
            tar_file = os.path.join(raws_dir , "{}/{}.gz.parquet".format(_year, _month))
            tar_dbfs = os.path.join(raws_dbfs, "{}/{}.gz.parquet".format(_year, _month))
            if check_file_exist(tar_file):
                continue
            fare_dbfs = os.path.join(csvs_dbfs, "{}/trip_fare_{}.csv".format(_year, _month))
            trip_dbfs = os.path.join(csvs_dbfs, "{}/trip_data_{}.csv".format(_year, _month))

            info_start(_year, _month)

            _fare_df = spark.read.option("header", True).csv(fare_dbfs)
            _trip_df = spark.read.option("header", True).csv(trip_dbfs)

            _fare_df = process_fare(_fare_df)
            _trip_df = process_trip(_trip_df)

            rs_df = _trip_df.join(_fare_df, ["medallion", "hack_license", "pickup_datetime"])
            rs_df.repartition(200)\
                .write.mode("overwrite")\
                .option("compression", "gzip")\
                .parquet(tar_dbfs)
            info_end(_year, _month)

#%%

combine_raw_data()
