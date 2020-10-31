# %%

import urllib
from zipfile import ZipFile
import os

# %%

# Set up modes and dirs
overwrite = False
databricks = False
if not databricks:
    from util import folder

    data_dir = folder.DATA_DIR
else:
    data_dir = "/dbfs/mnt/group01"

shap_path = os.path.join(data_dir, "nyc/zone")
node_path = os.path.join(data_dir, "nyc/map/node_raw.csv")
zone_path = os.path.join(data_dir, "nyc/map/node_zone.csv")

import geopandas as gpd

nyc_shp = gpd.read_file(shap_path)
nyc_shp = nyc_shp.to_crs(epsg=4326)

import pandas as pd

df = pd.read_csv(node_path)

_geo_df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["x"], df["y"]))
_geo_df = gpd.sjoin(_geo_df, nyc_shp, how="inner", op="intersects")

pd.DataFrame(_geo_df.drop(columns='geometry'))[["osmid", "x", "y", "LocationID"]].to_csv(zone_path, index=False)