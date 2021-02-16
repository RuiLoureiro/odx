import argparse
import datetime
import pandas as pd
from rich import print

from odx import config


def load_afc(mode, path):
    df = pd.read_feather(path)
    df["mode"] = mode
    return df


def filter_df(
    df,
    start_date=None,
    end_date=None,
    start_time=None,
    end_time=None,
):
    if start_date and end_date:
        if start_time and end_time:
            df = df[
                (
                    df.timestamp
                    >= datetime.datetime.combine(start_date, start_time)
                )
                & (
                    df.timestamp
                    <= datetime.datetime.combine(end_date, end_time)
                )
            ]
        else:
            df = df[
                (df.timestamp.dt.date >= start_date)
                & (df.timestamp.dt.date <= end_date)
            ]
    df = df.reset_index().drop("index", axis=1)
    return df


def get_combined_afc(
    afc_sources={
        "bus": config.PROCESSED_BUS_AFC_PATH,
        "metro": config.PROCESSED_METRO_AFC_PATH,
    },
    start_date=datetime.date(2019, 10, 1),
    end_date=datetime.date(2019, 10, 31),
    start_time=datetime.time(0, 0, 0),
    end_time=datetime.time(23, 59, 59),
    save=True,
    output_path=None,
):
    dfs = []
    for mode, path in afc_sources.items():
        print(f"Loading {mode} AFC from {path}")
        dfs.append(load_afc(mode, path))

    print(f"Concatenating and sorting {len(dfs)} AFC dataframes..")
    afc = pd.concat(dfs, ignore_index=True)
    afc = afc.sort_values(by="timestamp", ignore_index=True)

    print(f"Filtering AFC for rows between dates {start_date} and {end_date}")
    if start_time and end_time:
        print(f"and between times {start_time} and {end_time}")
    subset_afc = filter_df(afc, start_date, end_date, start_time, end_time)

    print(subset_afc.timestamp.iloc[0])

    if save:
        if output_path is None:
            output_path = f"combined_afc_{str(subset_afc.timestamp.iloc[0]).replace(' ', '_')}__{str(subset_afc.timestamp.iloc[-1]).replace(' ', '_')}"
        output_path += ".feather"
        print(f"Saving combined dataframe to {output_path}")
        subset_afc.to_feather(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process CARRIS AFC")

    parser.add_argument(
        "-sd",
        "--start-date",
        type=str,
        help="Start date in 'd-m-Y' format",
        required=False,
    )
    parser.add_argument(
        "-ed",
        "--end-date",
        type=str,
        help="Start date in 'd-m-Y' format",
        required=False,
    )
    parser.add_argument(
        "-st",
        "--start-time",
        type=str,
        help="Start time in 'H:M:S' format",
        required=False,
    )
    parser.add_argument(
        "-et",
        "--end-time",
        type=str,
        help="End time in 'H:M:S' format",
        required=False,
    )
    parser.add_argument("-o", "--output", help="output path", default=None)

    args = parser.parse_args()

    date_parser = (
        lambda s: None
        if s is None
        else datetime.date(*[int(s) for s in s.split("-")][::-1])
    )
    time_parser = (
        lambda s: None
        if s is None
        else datetime.time(*[int(s) for s in s.split(":")])
    )

    start_date = date_parser(args.start_date)
    end_date = date_parser(args.end_date)
    start_time = time_parser(args.start_time)
    end_time = time_parser(args.end_time)

    get_combined_afc(
        start_date=start_date,
        end_date=end_date,
        start_time=start_time,
        end_time=end_time,
        output_path=args.output,
    )
