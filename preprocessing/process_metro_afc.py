import json
import argparse
from pathlib import Path
import pandas as pd
from rich import print
from odx import config

metro_col_mapping = {
    "date": "FECHA",
    "time": "HORA",
    "stop_id": "ESTACAO",
    "card_id": "NUM_SER",
    "way": "E_S"
}


def process_metro_afc(
    path: str,
    col_mapping: str = metro_col_mapping,
    metro_stop_mapping_path=config.METRO_STOP_MAPPING_PATH,
    encoding: str = "latin1",
    sep: str = ";",
    output_path: str = None,
    **kwargs,
):
    path = Path(path)

    assert path.is_file(), "No such file {path}"

    print(f"Reading stop mapping from {metro_stop_mapping_path}..")
    with open(metro_stop_mapping_path, 'r') as f:
        metro_stop_mapping = json.load(f)

    converters = {
        col_mapping["way"]: {
            'E': 'IN',
            'S': 'OUT',
        }.get,
    }

    print(f"Reading raw CSV from {path}..\n")
    df = pd.read_csv(
        path,
        usecols=col_mapping.values(),
        parse_dates=[[col_mapping["date"], col_mapping["time"]]],
        converters=converters,
        encoding='latin1',
        sep=';',
        nrows=100,
    )

    print("Applying col mapping..")
    inv_col_mapping = {v: k for k, v in col_mapping.items()}

    inv_col_mapping['_'.join([col_mapping["date"], col_mapping["time"]])] = "timestamp"
    df.rename(columns=inv_col_mapping, inplace=True)

    df["stop_id"] = df["stop_id"].apply(lambda x: x[:2])  # SP1 -> SP

    df["stop_id"] = df["stop_id"].map(metro_stop_mapping)  # SP -> M46

    df["stop_id"] = df["stop_id"].apply(lambda x: x[1:])  # M46 -> 46

    df["stop_id"] = df["stop_id"].astype(int)

    assert len(df) == len(df.stop_id.dropna()), "Not every stop has a mapping, aborting.."

    output_path = (
        path.parent / (path.stem + "_processed.feather")
        if output_path is None
        else output_path
    )

    print(f"Saving processed dataframe to {output_path}..")
    df.to_feather(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process METRO AFC")
    parser.add_argument("path", type=str, nargs=1, help="input (raw) afc path")
    parser.add_argument("-o", "--output", help="output path", default=None)

    args = parser.parse_args()
    input_path = args.path[0]
    output_path = args.output

    process_metro_afc(input_path, metro_col_mapping, output_path=output_path)
