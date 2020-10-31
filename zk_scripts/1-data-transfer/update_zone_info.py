# Add zone center


# %%

import urllib
from zipfile import ZipFile
import geopandas as gpd
import os
import osmnx as ox

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
map_path = os.path.join(data_dir, "nyc/map/NYC.mph")
nyc_map = ox.load_graphml(map_path)

nyc_shp = gpd.read_file(shap_path)
nyc_shp = nyc_shp.to_crs(epsg=4326)
nyc_shp['center_point'] = nyc_shp['geometry'].centroid
nyc_shp["center_x"] = nyc_shp.center_point.map(lambda p: p.x)
nyc_shp["center_y"] = nyc_shp.center_point.map(lambda p: p.y)

the_nodes = ox.get_nearest_nodes(nyc_map, nyc_shp["center_x"], nyc_shp["center_y"], method='balltree')
nyc_shp["center_node"] = the_nodes
nyc_shp = nyc_shp[["LocationID", "center_x", "center_y", "center_node"]]
import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

nyc_shp.to_csv(os.path.join(data_dir, "nyc/zone/zone_center.csv"), index=False)

import pandas as pd
