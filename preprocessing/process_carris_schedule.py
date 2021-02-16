"""
Processes carris .xlsx file containing stop and route data, and outputs two json files
This file is an alternative to the GTFS, and is compatible with the carris AFC data.
"""
import json
from pathlib import Path
from collections import defaultdict
from rich import print
import pandas as pd
import argparse

from odx.bus_schedule import BusStop, BusRoute
from odx.validation import validate_stops
from odx import config


class RawColumnNames:
    stop_id = "Código Paragem"
    stop_lat = "Latitude"
    stop_lon = "Longitude"
    stop_name = "Designação"

    route_id = "Carreira"
    route_variant = "Variante"
    route_direction = "Sentido"


def process_carris_excel(path, output_dir):
    print(f"Reading excel from {Path(path).resolve()}..")
    routes_df = pd.read_excel(path).dropna(how="all")

    stops = []

    unique_stops_ids = routes_df[RawColumnNames.stop_id].unique()

    print("Processing stops..")

    for sid in unique_stops_ids:

        # get relevant stop information
        stop_df = routes_df.loc[routes_df[RawColumnNames.stop_id] == sid][
            [
                RawColumnNames.stop_lat,
                RawColumnNames.stop_lon,
                RawColumnNames.stop_name,
            ]
        ]
        if len(stop_df.drop_duplicates()) > 1:
            print(
                f"[red]Stop {sid} has more multiple entries, using first appearance"
            )

        row = stop_df.iloc[0]

        stops.append(
            BusStop(
                stop_id=int(sid),
                stop_name=str(row[RawColumnNames.stop_name]),
                stop_lat=float(row[RawColumnNames.stop_lat]),
                stop_lon=float(row[RawColumnNames.stop_lon]),
            )
        )

    validate_stops(stops)

    stops.sort(key=lambda s: s.stop_id)
    stops_path = Path(f"{output_dir}/stops.json")

    print(f"Writing {len(stops)} processed stops to {stops_path.resolve()}")
    with open(str(stops_path), "w") as fout:
        json.dump(stops, fout, default=lambda x: x.to_dict(), indent=2)

    routes_dict = defaultdict(list)

    print(f"Processing routes..")
    for idx, row in routes_df.iterrows():
        route_id = row[RawColumnNames.route_id]
        route_variant = row[RawColumnNames.route_variant]
        direction = row[RawColumnNames.route_direction]

        if direction == "A":
            direction = BusRoute.Directions.ASC
        elif direction == "D":
            direction = BusRoute.Directions.DESC
        elif direction == "C":
            direction = BusRoute.Directions.CIRC
        else:
            direction = BusRoute.Directions.UNDEFINED

        routes_dict[route_id, direction, route_variant].append(
            int(row[RawColumnNames.stop_id])
        )

    routes = []

    for key, values in routes_dict.items():
        route_id, direction, route_variant = key

        routes.append(
            {
                "route_id": str(route_id),
                "route_direction": str(direction),
                "route_variant": int(route_variant),
                "route_stop_ids": values,
            }
        )

    routes_path = Path(f"{output_dir}/routes.json")

    print(f"Writing {len(routes)} processed routes to {routes_path.resolve()}")
    with open(routes_path, "w") as fp:
        json.dump(routes, fp, default=lambda x: x.to_dict())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process CARRIS excel")
    parser.add_argument("path", type=str, nargs=1, help="excel path")
    parser.add_argument(
        "-o",
        "--output",
        help="output directory",
        default=config.PROCESSED_DATA_PATH,
    )

    args = parser.parse_args()
    input_path = args.path[0]
    output_dir = args.output
    process_carris_excel(input_path, output_dir)
