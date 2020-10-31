#%%

import os
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib

#%%

# Set up modes and dirs
databricks = False
overwrite  = False
is_yellow  = False
yellow     = "yellow" if is_yellow else "foil"

#%%

start = 0
end   = 5000
step  = 500

#%%

if not databricks:
    data_dir = "/Users/kzmain/LSDE/data"
else:
    data_dir = "/dbfs/mnt/group01"
#%%
shape_map_path = os.path.join(data_dir,  "nyc/zone")
soa_shape_map = gpd.read_file(shape_map_path)
soa_shape_map_geo = soa_shape_map.to_crs("EPSG:4326")
soa_shape_map_geo.to_file(data_dir + "/zone.json", driver='GeoJSON')
