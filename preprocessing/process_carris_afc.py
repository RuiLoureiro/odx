import argparse
from pathlib import Path
import pandas as pd
from rich import print
from odx import config

carris_col_mapping = {
    "timestamp": "Data/Hora",
    "card_id": "NºSerie",
    "stop_id": "IDParagem",
    "route_direction": "Sentido",
    "route_id": "Carreira",
    "route_variant": "Variante",
    "stop_name": "Designação",
    "stop_number": "Paragem",
    # 'description': 'Descrição'
}


def process_carris_afc(
    path: str,
    col_mapping: str = carris_col_mapping,
    encoding: str = "latin1",
    sep: str = ";",
    output_path: str = None,
    **kwargs,
):
    """
    Processes carris raw AFC csv file.
    """
    path = Path(path)

    assert path.is_file(), "No such file {path}"
    # Final column names
    cols = [
        "timestamp",
        "card_id",
        "stop_id",
        "stop_name",
        "route_id",
        "route_variant",
        "route_direction",
        "stop_number",
    ]

    # check if col_mapping has all the necessary cols
    for col in cols:
        if col not in col_mapping:
            raise ValueError(f"Missing column in col_mapping: '{col}'")

    # list columns to extract from csv
    usecols = [col_mapping[col] for col in cols]

    dtype = {
        col_mapping["route_direction"]: str,
        col_mapping["route_id"]: str,
        col_mapping["route_variant"]: "Int64",
        col_mapping["stop_name"]: str,
        col_mapping["card_id"]: "Int64",
        # col_mapping["stop_id"]: 'Int64',
        col_mapping["stop_number"]: "Int64",
    }

    def convert_stop_id(sid):
        if sid == "":
            return None
        try:
            return int(sid.split(",")[0])
        except ValueError:
            return None

    parse_dates = [col_mapping["timestamp"]]

    header = pd.read_csv(path, nrows=1, encoding=encoding, sep=sep)
    print(f"Raw Columns: {list(header.columns)}\n")

    print(f"Reading raw CSV from {path}..\n")
    try:
        df = pd.read_csv(
            path,
            usecols=usecols,
            dtype=dtype,
            infer_datetime_format=True,
            parse_dates=parse_dates,
            header=0,
            encoding=encoding,
            sep=sep,
            converters={col_mapping["stop_id"]: convert_stop_id},
            **kwargs,
        )
    except ValueError as e:
        wrong_cols = []
        for col in col_mapping.values():
            if col not in header.columns:
                wrong_cols.append(col)
        if wrong_cols:
            raise RuntimeError(f"Columns not found in csv: {wrong_cols}")
        raise e

    # standard column naming
    inv_col_mapping = {v: k for k, v in col_mapping.items()}

    print("Renaming columns..")
    df.rename(columns=inv_col_mapping, inplace=True)
    print(f"Missing values:\n{df.isnull().sum()/len(df)}")

    print("Dropping duplicates..")
    # drop entries that have the same timestamp, card_id pair
    df.drop_duplicates(subset=["timestamp", "card_id"], inplace=True)
    print("Sorting by timestamp..")
    # sort by timestamp
    df["route_direction"] = df["route_direction"].fillna("")
    df["stop_id"] = df["stop_id"].astype("Int64")
    df.sort_values(by="timestamp", ignore_index=True, inplace=True)

    cols_to_save = [
        "timestamp",
        "card_id",
        "stop_id",
        "route_id",
        "route_variant",
        "route_direction",
        "stop_number",
    ]
    output_path = (
        path.parent / (path.stem + "_processed.feather")
        if output_path is None
        else output_path
    )

    print(f"Saving processed dataframe to {output_path}")

    df[cols_to_save].to_feather(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process CARRIS AFC")
    parser.add_argument("path", type=str, nargs=1, help="input (raw) afc path")
    parser.add_argument("-o", "--output", help="output path", default=config.PROCESSED_BUS_AFC_PATH)

    args = parser.parse_args()
    input_path = args.path[0]
    output_path = args.output

    print("[red]Assuming data is from October 2019, which has the last row corrupted.\nIf this is not the case, remove the 'nrows' argument from the call to 'process_carris_afc'\n")
    process_carris_afc(input_path, carris_col_mapping, output_path=output_path, nrows=6282065)
