# %%
import numpy as np
import os
import pandas
import osmnx as ox
from pathlib import Path
import geopandas as gpd
from pyspark.sql import SparkSession

from pyspark.sql import functions as func

# %%

# Set up modes and dirs
databricks = False
overwrite = False
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

fr_file = fr_dbfs = (data_dir + "/{}".format(yellow) + "/raw/{}/{}.gz.parquet")
to_file = to_dbfs = (data_dir + "/{}".format(yellow) + "/drop_edges/{}/{}.gz.parquet")
to_dir = (data_dir + "/{}".format(yellow) + "/drop_edges/{}")
if databricks:
    fr_dbfs = fr_dbfs.replace("/dbfs", "")
    to_dbfs = to_dbfs.replace("/dbfs", "")
# %%

if is_yellow:
    start_year = 2009
    end_year = 2016
else:
    start_year = 2010
    end_year = 2013

# %%

year_range = range(start_year, end_year + 1)
month_range = range(1, 13)


# %%

def check_file_exist(_path):
    if os.path.exists(_path) and not overwrite:
        print("[SYSTEM]: File exists: {}".format(_path))
        return True
    else:
        return False


# %% Data comes from osmnx map

w_lon = -74.2463  # left bound
e_lon = -73.7141  # right bound
n_lat = 40.9166  # up bound
s_lat = 40.4767  # down bound

drop_lon = "dropoff_longitude"
drop_lat = "dropoff_latitude"
pick_lon = "pickup_longitude"
pick_lat = "pickup_latitude"

# %%

map_path = os.path.join(data_dir, "nyc/map/NYC.mph")
nyc_map = ox.load_graphml(map_path)

# %%
def get_file_list(_path):
    _parquet_file_list = []
    for _root, _dirs, _files in os.walk(_path, topdown=False):
        for _name in _files:
            _file_name = os.path.join(_root, _name)
            if Path(_file_name).suffix == '.parquet':
                _parquet_file_list.append(_file_name)
    return _parquet_file_list


# %%
def read_from_parquet(_file_list):
    _full_df = pandas.concat(pandas.read_parquet(_parquet_file) for _parquet_file in _file_list)
    _full_df = filter_location(_full_df)
    _full_df = filter_duration(_full_df)
    _full_df = filter_distance(_full_df)
    # print(_full_df.shape) (14863778, 16)
    _full_df = _full_df[_full_df['valid']]
    # print(_full_df.shape) (14507105, 16)

    _pick_df = _full_df[[pick_lon, pick_lat]].drop_duplicates()
    _drop_df = _full_df[[drop_lon, drop_lat]].drop_duplicates()
    return _pick_df, _drop_df


# %%
def filter_location(_in_df):
    _in_df = _in_df.round({drop_lat: 4, drop_lon: 4, pick_lat: 4, pick_lon: 4})
    _in_df['valid'] = True
    _in_df.loc[_in_df[pick_lon] > e_lon, 'valid'] = False
    _in_df.loc[_in_df[pick_lon] < w_lon, 'valid'] = False
    _in_df.loc[_in_df[drop_lon] > e_lon, 'valid'] = False
    _in_df.loc[_in_df[drop_lon] < w_lon, 'valid'] = False
    _in_df.loc[_in_df[pick_lat] > n_lat, 'valid'] = False
    _in_df.loc[_in_df[pick_lat] < s_lat, 'valid'] = False
    _in_df.loc[_in_df[drop_lat] > n_lat, 'valid'] = False
    _in_df.loc[_in_df[drop_lat] < s_lat, 'valid'] = False

    _in_df.loc[_in_df[pick_lon].isnull(), 'valid'] = False
    _in_df.loc[_in_df[pick_lon].isnull(), 'valid'] = False
    _in_df.loc[_in_df[drop_lon].isnull(), 'valid'] = False
    _in_df.loc[_in_df[drop_lon].isnull(), 'valid'] = False
    _in_df.loc[_in_df[pick_lat].isnull(), 'valid'] = False
    _in_df.loc[_in_df[pick_lat].isnull(), 'valid'] = False
    _in_df.loc[_in_df[drop_lat].isnull(), 'valid'] = False
    _in_df.loc[_in_df[drop_lat].isnull(), 'valid'] = False
    return _in_df


# %%
def filter_duration(_in_df):
    duration_delta = "duration"
    duration_secod = "duration_second"
    _in_df[duration_delta] = _in_df["dropoff_datetime"] - _in_df["pickup_datetime"]
    _in_df[duration_secod] = _in_df[duration_delta] / np.timedelta64(1, 's')
    _in_df.loc[_in_df[duration_secod] < 45, 'valid'] = False
    return _in_df.drop(columns='duration')


# %%
def filter_distance(_in_df):
    _in_df.loc[_in_df["trip_distance"] < 0.45, 'valid'] = False
    return _in_df


# %%
def get_taxi_edges(_in_df, mode, _year, _month):
    _lons = _in_df["{}_longitude".format(mode)]
    _lats = _in_df["{}_latitude".format(mode)]
    _result = ox.get_nearest_edges(nyc_map, _lons, _lats, method="kdtree")
    _u = []
    _v = []
    for _r in _result:
        _u.append(int(_r[0]))
        _v.append(int(_r[1]))

    _in_df["{}_u".format(mode)] = _u
    _in_df["{}_v".format(mode)] = _v
    if not os.path.exists(to_dir.format(_year)):
        os.makedirs(to_dir.format(_year))
    _in_df.to_parquet(to_file.format(_year, _month), compression='gzip', index=False)
    return _in_df

# %%

def process():
    info_strin = "____________________________{}_PROCESS_{}_{}____________________________"
    info_title = lambda _mode, _y, _m: print(info_strin.format(_mode.upper(), _y, _m))
    info_start = lambda _y, _m: print("[SYSTEM]: Start  {}-{}".format(_y, _m))
    info_end = lambda _y, _m: print("[SYSTEM]: Finish {}-{}".format(_y, _m))
    for year in year_range:
        for month in month_range:
            if not os.path.exists(fr_file.format(year, month)):
                continue
            info_title(yellow, year, month)
            if check_file_exist(to_file.format(year, month)):
                continue
            info_start(year, month)
            from datetime import datetime
            s = datetime.now()
            # Get file location of the parquet
            raw_file_path = fr_file.format(year, month)

            # Read in parquet file by year-month
            parquet_file_list = get_file_list(raw_file_path)
            pick_df, drop_df = read_from_parquet(parquet_file_list)

            # Get taxi edges
            print("[SYSTEM]: GET TAXI EDGE")
            get_taxi_edges(drop_df, drop_off, year, month)
            info_end(year, month)


process()
