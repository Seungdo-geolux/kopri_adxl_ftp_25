import argparse
import os.path

import pandas as pd


def process_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert binary raw data to CSV/XLSX files using multicores"
    )
    parser.add_argument(
        "-f",
        "--filepath",
        default="./Saved_Data_KOA/20250320",
        type=str,
        help="input csv filepath: folder or file (default folder: ./)",
    )
    parser.add_argument(
        "-s",
        "--timestamp",
        action="store_true",
        help="existence of timestamp",
    )

    args = parser.parse_args()

    return args


def validate_csv_file(csv_file: str, timestamp: bool) -> None:
    df = pd.read_csv(csv_file)
    if timestamp:
        if "Time" not in df.columns:
            print(f"{csv_file} does not have a column named 'Time'")
            return

        diff = df["Time"].diff()
        diff.drop(diff.index[0], inplace=True)
        result = diff[diff != 1]
        final = result[result != -999]
        if final.empty:
            print(f"{csv_file} validated")
        else:
            print(
                f"{csv_file} has invalid data at rows {[x + 2 for x in final.index.tolist()]}"
            )

    # if df.shape[0] != 60000:
    #     print(f"{csv_file} does not contain 60,000 samples")
    print(f"{csv_file} contains {df.shape[0]} samples")


def main(args: argparse.Namespace) -> None:
    if os.path.isfile(args.filepath):
        validate_csv_file(args.filepath)
    elif os.path.isdir(args.filepath):
        csv_files = [
            f
            for f in os.listdir(args.filepath)
            if f.endswith(".csv") and not f.endswith("_solar.csv")
        ]

        if not csv_files:
            print(f"File not found in {args.filepath}")
        else:
            for file in csv_files:
                validate_csv_file(
                    os.path.normpath(os.path.join(args.filepath, file)), args.timestamp
                )


if __name__ == "__main__":
    main(process_args())
