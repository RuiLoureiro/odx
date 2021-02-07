from pathlib import Path

home = str(Path.home())


DATA_PATH = f"{home}/tese/repo/data"  # change appropriately
RAW_DATA_PATH = f"{DATA_PATH}/raw"
PROCESSED_DATA_PATH = f"{DATA_PATH}/processed"


# BUS
BUS_STOPS_PATH = f"{PROCESSED_DATA_PATH}/stops.json"
BUS_ROUTES_PATH = f"{PROCESSED_DATA_PATH}/routes.json"
BUS_STAGE_TIMES_GTFS_PATH = f"{PROCESSED_DATA_PATH}/bus_stage_times_gtfs.json"
BUS_STOP_TIME = 30


# GTFS
METRO_GTFS_PATH = f"{RAW_DATA_PATH}/gtfs_metro_10_2019"
CARRIS_GTFS_PATH = f"{RAW_DATA_PATH}/gtfs_carris_02_2020"


# METRO
METRO_STOP_MAPPING_PATH = f"{PROCESSED_DATA_PATH}/metro_stop_mapping.json"

# AFC
PROCESSED_BUS_AFC_PATH = f"{PROCESSED_DATA_PATH}/afc_carris_10_2019.feather"
PROCESSED_METRO_AFC_PATH = f"{PROCESSED_DATA_PATH}/afc_metro_10_2019.feather"
