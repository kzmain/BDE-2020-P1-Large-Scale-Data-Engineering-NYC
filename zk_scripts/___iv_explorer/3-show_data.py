# %%

# Set up modes and dirs
import glob
from pyspark.sql import SparkSession
from bokeh.plotting import gmap
from bokeh.models import ColumnDataSource, GMapOptions
from bokeh.io import output_file, show
import matplotlib.pyplot as plt
import matplotlib as mpl

# mpl.rcParams['figure.dpi'] = 1000
# %%

overwrite = False
databricks = False
is_yellow = False

yellow = "yellow" if is_yellow else "foil"
pick_up = "pickup"
drop_off = "dropoff"

# %%

if not databricks:
    data_dir = "../data"
    spark = SparkSession.builder \
        .appName("Your App Name") \
        .config("spark.executor.memory", "14g") \
        .config("spark.driver.memory", "14g") \
        .getOrCreate()
else:
    data_dir = "/dbfs/mnt/group01"

cluster_file = cluster_dbfs = (data_dir + "/{}".format(yellow) + "/raw/2010/1.gz.parquet")
cluster_center_df = spark.read.parquet(cluster_dbfs) \
    .select(["pickup_longitude", "pickup_latitude"]) \
    .dropDuplicates()\
    .limit(100000)

# file to save the model
output_file("points_on_map.html")
# configuring the Google map
lat = 40.6929
lng = -73.8032
map_type = "hybrid"
zoom = 11
google_map_options = GMapOptions(lat=lat,
                                 lng=lng,
                                 map_type=map_type,
                                 zoom=zoom)

# generating the Google map
google_api_key = "AIzaSyBpo4t6BxgT9Z_gXPmnZPSIorxOCAUnxiI"
title = "Delhi"
google_map = gmap(google_api_key,
                  google_map_options,
                  title=title)

cluster_center_df = cluster_center_df.toPandas()
# the coordinates of the glyphs
source = ColumnDataSource(
    data=dict(lat=cluster_center_df['pickup_latitude'].tolist(),
              lon=cluster_center_df['pickup_longitude'].tolist()))

# generating the glyphs on the Google map
x = "lon"
y = "lat"
size = 10
fill_color = "red"
fill_alpha = 1
google_map.square(x=x,
                  y=y,
                  size=size,
                  fill_color=fill_color,
                  fill_alpha=fill_alpha,
                  source=source)

# displaying the model
show(google_map)
