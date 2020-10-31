from os import path
from pathlib import Path

PROJ_DIR = Path(__file__).parent.parent

DATA_DIR = path.join(PROJ_DIR, "data")
YELL_DIR = path.join(DATA_DIR, "yellow_download")
TAXI_DIR = path.join(DATA_DIR, "taxi")
FOIL_DIR = path.join(TAXI_DIR, "FOIL")


MISC_DIR = path.join(DATA_DIR, "misc")
