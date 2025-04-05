import argparse
import os.path

import numpy as np
import pandas as pd

import minilzo


def parse_input_data(
    byte_data: bytes, range: int, datawidth: int, tsEable: bool = True
) -> pd.DataFrame:
    # parse input data and convert them to DataFrame

    if byte_data[0:4] == bytes("MLZO", "utf-8"):
        decompressed_data = decompress_data(byte_data)
        if decompressed_data:
            byte_data = decompressed_data
        else:
            print("Decompression error")
            return pd.DataFrame()

    if datawidth == 16 or datawidth == 32:
        if tsEable:
            sensor_max_value = 0x8000 if datawidth == 16 else 0x80000
            scale_factor = range / sensor_max_value
            dt = np.dtype(
                [
                    ("timestamp", "<u2"),
                    ("values", "<3i2") if datawidth == 16 else ("values", "<3i4"),
                ],
            )

            array_data = np.frombuffer(byte_data, dtype=dt)
            sensor_data = scale_factor * array_data["values"].astype("float64")
            root_sum_of_squares = np.sqrt(np.sum(np.square(sensor_data), axis=1))

            df_sensor_data = pd.DataFrame(
                np.hstack(
                    (
                        array_data["timestamp"].reshape(-1, 1),
                        sensor_data,
                        root_sum_of_squares.reshape(-1, 1),
                    ),
                ),
                columns=["Timestamp", "ACC_X", "ACC_Y", "ACC_Z", "Total"],
                # columns=["ACC_X", "ACC_Y", "ACC_Z", "Total"],
            )
            return df_sensor_data
        else:
            dt = np.dtype(
                [
                    ("values", "<3i2") if datawidth == 16 else ("values", "<3i4"),
                ],
            )

            array_data = np.frombuffer(byte_data, dtype=dt)
            sensor_data = scale_factor * array_data["values"].astype("float64")
            root_sum_of_squares = np.sqrt(np.sum(np.square(sensor_data), axis=1))

            df_sensor_data = pd.DataFrame(
                np.hstack(
                    (
                        sensor_data,
                        root_sum_of_squares.reshape(-1, 1),
                    ),
                ),
                columns=["ACC_X", "ACC_Y", "ACC_Z", "Total"],
            )
            timestamp = np.arange(0, len(df_sensor_data))
            df_timestamp = pd.DataFrame(
                timestamp,
                columns=["Time"],
            )

            df = pd.concat(
                [
                    df_timestamp,
                    df_sensor_data,
                ],
                axis=1,
            )
            return df

    else:  # FOR datawidth == 24
        sensor_max_value = 0x80000
        scale_factor = range / sensor_max_value
        bytes_per_sample = 2 + 3 * 3
        num_blocks = len(byte_data) // bytes_per_sample

        timestamp = []
        adc_data = []

        for i in range(num_blocks):
            offset = bytes_per_sample * i
            timer_value = int.from_bytes(
                byte_data[offset : offset + 2],
                byteorder="little",
                signed=False,
            )
            timestamp.append(timer_value)

            # each ADC value is stored in 3 bytes
            offset += 2
            reduced_adc_values = [
                byte_data[offset + k : offset + k + 3] for k in range(0, 3 * 3, 3)
            ]

            values = []
            for adc_value in reduced_adc_values:
                # convert 3-byte value to 4-byte value with the sign extension in mind
                values.append(
                    int.from_bytes(
                        adc_value + (b"\xff" if adc_value[2] & 0x80 else b"\x00"),
                        byteorder="little",
                        signed=True,
                    )
                )
            adc_data.append(values)

        df_timestamp = pd.DataFrame(
            timestamp,
            columns=["Time"],
        )
        df_sensor_data = scale_factor * pd.DataFrame(
            adc_data,
            columns=["ACC_X", "ACC_Y", "ACC_Z"],
        )
        df_sensor_data["Total"] = (df_sensor_data**2).sum(axis=1) ** 0.5

        df = pd.concat(
            [
                df_timestamp,
                df_sensor_data,
            ],
            axis=1,
        )

    return df


def decompress_data(byte_data: bytes) -> bytes:
    raw_data_size = int.from_bytes(byte_data[4:8], byteorder="little")
    decompressed_data = bytearray()
    read_idx = 8
    print(f"This is a compressed file (original data size: {raw_data_size:,} bytes)")

    block_num = 0
    num_blocks_checksum_error = 0
    num_blocks_uncompressed = 0
    while True:
        file_compression_flag = byte_data[read_idx]
        checksum = byte_data[read_idx + 1]
        block_size = int.from_bytes(
            byte_data[read_idx + 2 : read_idx + 4], byteorder="little"
        )
        read_idx += 4
        block_size_padding = block_size + (4 - block_size % 4) % 4

        if sum(byte_data[read_idx : read_idx + block_size]) & 0xFF == checksum:
            if file_compression_flag:
                decompressed_block = minilzo.decompress_block(
                    byte_data[read_idx : read_idx + block_size]
                )
                decompressed_data += decompressed_block
                # print(
                #     f"decompress block {block_num}: {block_size:,} => {len(decompressed_block):,} bytes"
                # )
            else:
                num_blocks_uncompressed += 1
                decompressed_data += byte_data[read_idx : read_idx + block_size]
                f"block {block_num} is uncompressed"
        else:
            num_blocks_checksum_error += 1
            print(
                f"checksum error at block {block_num} with compressed size {block_size:,}"
            )

        block_num += 1
        read_idx += block_size_padding
        if read_idx >= len(byte_data):
            break

    if decompressed_data:
        if num_blocks_checksum_error:
            print(
                f"The number of blocks with checksum error: {num_blocks_checksum_error}"
            )
            return bytes()
        # print(
        #     f"\nThe number of blocks with uncompressed data: {num_blocks_uncompressed}\n"
        #     f"Compressed file size: {(100 * len(byte_data) / len(decompressed_data)):.1f} % of the raw data"
        # )
    return bytes(decompressed_data)


def process_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert binary raw data to a CSV/XLSX file"
    )
    parser.add_argument(
        "-f",
        "--filename",
        default="intput.dat",
        type=str,
        help="input filename (default: input.dat)",
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
    input_file = args.filename

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
