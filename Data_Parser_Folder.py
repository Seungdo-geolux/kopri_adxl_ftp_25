import argparse
import glob
import os
import os.path

from KOPRI_Data_Parse import parse_input_data

# CSV 파일들이 저장된 폴더 경로
folder_base = r"C:\myWork\python\kopri_adxl_ftp_25\Saved_Data_KOA"
folder_date = "20250410"
folder_path = os.path.join(folder_base, folder_date)
file_pattern = os.path.join(folder_path, "*_20.csv")
csv_files = sorted(glob.glob(file_pattern))


def process_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert binary raw data to a CSV/XLSX file"
    )
    parser.add_argument(
        "-f",
        "--folderpath",
        default="./",
        type=str,
        help="input folder path (default: ./ )",
    )
    parser.add_argument(
        "-p",
        "--pattern",
        default="*.dat",
        type=str,
        help="search pattern (default: *.dat )",
    )
    parser.add_argument(
        "-o",
        "--outfolder",
        default="",
        type=str,
        help="output folder name (default: input file folder)",
    )
    parser.add_argument(
        "-r",
        "--range",
        default=4,
        type=int,
        choices=(2, 4),
        help="ADXL sensor full scale range (default: 4)",
    )
    parser.add_argument(
        "-d",
        "--datawidth",
        default=16,
        type=int,
        choices=(16, 24, 32),
        help="ADXL acceleration data width in bits per axis (default: 16)",
    )
    parser.add_argument(
        "-t",
        "--filetype",
        default="csv",
        type=str,
        choices=("csv", "xlsx"),
        help="type of output file (default: csv)",
    )
    args = parser.parse_args()

    return args


def main(args: argparse.Namespace) -> None:
    input_folderpath = args.folderpath
    file_pattern = os.path.join(input_folderpath, args.pattern)

    sorted_files = sorted(glob.glob(file_pattern))
    print(sorted_files, file_pattern, input_folderpath)

    for input_file in sorted_files:
        if args.outfolder:
            if not os.path.isdir(args.outfolder):
                os.makedirs(args.outfolder)

            output_file = os.path.join(
                args.outfolder,
                os.path.splitext(os.path.basename(input_file))[0] + "." + args.filetype,
            )
        else:
            output_file = os.path.splitext(input_file)[0] + "." + args.filetype

        try:
            # open the binary file in binary mode
            with open(input_file, "rb") as file:
                # read the entire content of the file
                byte_data = file.read()

            # process binary data
            df = parse_input_data(byte_data, args.range, args.datawidth)

            # save the DataFrame to Excel
            print(f"Converting {input_file} to {output_file}")
            if args.filetype == "xlsx":
                df.to_excel(output_file, index=False, engine="xlsxwriter")
            else:
                df.to_csv(output_file, index=False)
        # exception handling
        except FileNotFoundError:
            print(f"Error: File '{input_file}' not found.")
        except PermissionError:
            print(f"Error: File {output_file} is already in use so it cannot be saved.")
        except Exception as e:
            print(f"Error: An unexpected error occurred: {e}")
        else:
            print(f"File {output_file} has been succesfully created.")


if __name__ == "__main__":
    args = process_args()
    main(args)
