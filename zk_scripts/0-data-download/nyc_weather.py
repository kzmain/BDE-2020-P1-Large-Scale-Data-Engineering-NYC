#%%


#%%

import json
import re
import os
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, explode, when, lit
from pyspark.sql.types import ShortType, ArrayType, StringType

#%%

# Set up modes and dirs
overwrite = False
databricks = False
if not databricks:
    from util import folder

    data_dir = folder.DATA_DIR
    spark = SparkSession.builder.getOrCreate()
else:
    data_dir = "/dbfs/mnt/group01"

weather_dir = weather_dbfs = os.path.join(data_dir, "nyc/weather")
html_dir = html_dbfs = os.path.join(weather_dir, "html")
csv_dir = csv_dbfs = os.path.join(weather_dir, "csv")
par_dir = par_dbfs = os.path.join(weather_dir, "parquet")
if databricks:
    weather_dbfs = weather_dbfs.replace("/dbfs", "")
    html_dbfs = html_dbfs.replace("/dbfs", "")
    csv_dbfs = csv_dbfs.replace("/dbfs", "")
    par_dbfs = par_dbfs.replace("/dbfs", "")

dirs = [data_dir, weather_dir, html_dir, csv_dir]

#%%

for d in dirs:
    if not os.path.exists(d):
        os.makedirs(d)

#%%

s_year = 2009
e_year = 2020

s_month = 1
e_month = 12

url = "https://www.timeanddate.com/weather/usa/new-york/historic?month={}&year={}"
html = html_dir + "/{}.{}.html"


#%%

def check_file_exist(_path):
    if os.path.exists(_path) and not overwrite:
        print("[SYSTEM]: File exists: {}".format(_path))
        return True
    else:
        return False


#%%

def is_valid_detail(_details, _year, _month):
    if len(_details) == 0:
        return False

    _ts = _details[0]["date"] / 1000
    _dt = datetime.utcfromtimestamp(_ts)
    _y = _dt.year
    _m = _dt.month
    if _month != _m or _year != _y:
        return False
    return True


#%%

def write_html(_year, _month, _page):
    _file = open(html.format(_year, _month), "w+")
    _file.write(_page.text)
    _file.close()


#%%

def write_csv_head(_year, _month):
    _file = open(os.path.join(csv_dir, "{}_{}.csv".format(_year, _month)), "w+")
    _file.write("time_stamp,hour,day,month,year,high,low,baro,wind,wd,hum,weather\n")
    _file.close()


#%%

def write_csv(_time_stamp, _hour, _day, _month, _year, _temp_high, _temp_low, _baro, _wind, _wd, _hum, _description):
    _file = open(os.path.join(csv_dir, "{}_{}.csv".format(_year, _month)), "a+")
    _count = 0
    for _weather in _description:
        if _weather.rstrip() == "":
            continue
        if _count:
            break
        _w = _weather \
            .replace("partly ", "") \
            .replace("passing ", "") \
            .replace("mostly ", "") \
            .replace("broken ", "") \
            .replace("scattered ", "") \
            .replace("more ", "") \
            .replace(" than sun", "") \
            .replace("no weather data available", "NULL")
        _result = "{},{},{},{},{},{},{},{},{},{},{},{}\n" \
            .format(_time_stamp, _hour, _day, _month, _year,
                    _temp_high, _temp_low, _baro, _wind, _wd, _hum, _w)
        _file.writelines(_result)
        _count = _count + 1
    _file.close()


#%%

def get_script(_page):
    _soup = bs(_page.content, 'html.parser')
    _scripts = _soup.find_all('script')
    for _script in _scripts:
        _json_string = re.search(r'(?<=data=)({.*})', str(_script))
        if _json_string is not None:
            return json.loads(str(_script)[_json_string.start():_json_string.end()])["detail"]
    return None


def get_detail_info(_detail):
    _time_stamp = _detail["date"] / 1000
    date = datetime.utcfromtimestamp(_time_stamp)
    _hour = date.hour
    _day = date.day
    _month = date.month
    _year = date.year

    _temp_high = _detail["temp"] if "temp" in _detail.keys() else "NULL"
    _temp_low = _detail["templow"] if "templow" in _detail.keys() else "NULL"

    _baro = _detail["baro"]
    _wind = _detail["wind"]
    _wd = _detail["wd"]
    _hum = _detail["hum"]
    _description = _detail["desc"].lower().split(".")
    return _time_stamp, _hour, _day, _month, _year, _temp_high, _temp_low, _baro, _wind, _wd, _hum, _description


#%%

# Get weather from
def get_weather():
    info_title = lambda _y, _m: print("____________________________WEATHER_{}_{}____________________________".format(_y, _m))
    info_start = lambda _y, _m: print("[SYSTEM]: Start  {}-{}".format(_y, _m))
    info_end   = lambda _y, _m: print("[SYSTEM]: Finish {}-{}".format(_y, _m))

    for t_year in range(s_year, e_year + 1, 1):
        for t_month in range(s_month, e_month + 1, 1):
            # Request weather data page
            info_title(t_year, t_month)
            if check_file_exist(os.path.join(csv_dir, "{}_{}.csv".format(t_year, t_month))):
                continue
            page = requests.get(url.format(t_month, t_year))
            info_start(t_year, t_month)
            if page.status_code == 200:
                # Write backup files
                # Extract weather data by regx
                _details = get_script(page)
                if not is_valid_detail(_details, t_year, t_month):
                    continue
                write_html(t_year, t_month, page)
                write_csv_head(t_year, t_month)
                for _dl in _details:
                    ts, hour, day, month, year, high, low, baro, wind, wd, hum, description = get_detail_info(_dl)
                    write_csv(ts, hour, day, month, year, high, low, baro, wind, wd, hum, description)
            info_end(t_year, t_month)


# get_weather()


#%%

def list_hour(hour):
    res = []
    for h in range(hour, hour + 6):
        res.append(h)
    return res


spark_cube = udf(list_hour, ArrayType(ShortType()))


spark.read.option("header", True) \
    .csv("{}/*.csv".format(csv_dbfs)) \
    .withColumn("hour", col("hour").cast(ShortType())) \
    .withColumn("day", col("day").cast(ShortType())) \
    .withColumn("month", col("month").cast(ShortType())) \
    .withColumn("year", col("year").cast(ShortType())) \
    .withColumn("high", col("high").cast(ShortType())) \
    .withColumn("low", col("low").cast(ShortType())) \
    .withColumn("baro", col("baro").cast(ShortType())) \
    .withColumn("wind", col("wind").cast(ShortType())) \
    .withColumn("wd", col("wd").cast(ShortType())) \
    .withColumn("hum", col("hum").cast(ShortType())) \
    .withColumn("time_stamp", from_unixtime("time_stamp").alias("time_stamp")) \
    .withColumn("hour", spark_cube("hour")) \
    .withColumn("hour", explode("hour")) \
    .withColumn("weather", when(col("weather") == "NULL", lit(None)).otherwise(col("weather"))) \
    .write.mode("overwrite") \
    .option("compression", "gzip") \
    .parquet(par_dbfs)
