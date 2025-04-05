import io
import os
import subprocess
import time
from ftplib import FTP

import pandas as pd

from KOPRI_Data_Parse import parse_input_data

DATA_BITS = 16


class myFTP:
    def __init__(self, logger, device):
        self.logger = logger
        self.ftp_host = device["ftp_host"]
        self.ftp_user = device["ftp_user"]
        self.ftp_password = device["ftp_password"]
        self.buffer_size = device["buffer_size"]
        self.remote_folder = device["remote_folder"]
        self.remote_range = device["remote_range"]
        self.data_parsing = device.get("data_parsing", True)
        self.logger.debug(
            f"Created myFTP instance for host: {self.ftp_host}, user: {self.ftp_user}"
        )
        self.logger.debug(f"{self.data_parsing=}")

    def connection_test(self) -> bool:
        # 플랫폼에 따른 ping 옵션 설정: Windows는 "-n", 그 외는 "-c"
        ping_option = "-n" if os.name == "nt" else "-c"
        try:
            subprocess.check_output(["ping", ping_option, "1", self.ftp_host])
            self.logger.debug(f"{self.ftp_host} is reachable")
            return True
        except subprocess.CalledProcessError as err:
            self.logger.warning(f"{self.ftp_host} is unreachable")
            self.logger.error(str(err))
            return False

    def connect(self):
        try:
            self.ftp = FTP(self.ftp_host, timeout=5.0)
            self.ftp.login(user=self.ftp_user, passwd=self.ftp_password)
            self.ftp.set_pasv(True)
            self.logger.debug(f"{self.ftp_host} Connected")
        except Exception as err:
            self.logger.error(f"{self.ftp_host} Connection error: {str(err)}")
            raise

    def disconnect(self):
        try:
            self.ftp.quit()
            self.logger.debug(f"{self.ftp_host} Disconnected")
        except Exception as err:
            self.logger.error(f"{self.ftp_host} Disconnection error: {str(err)}")
            raise

    def download_files_from_ftp(self, fn, download_folder):
        # 원격 FTP 서버의 파일 경로(fn)를 분리하여 로컬 저장 경로 생성
        folder_path, file_name = os.path.split(fn)
        relative_folder_path = folder_path.lstrip("/\\")  # 선행 슬래시 제거
        save_folder = os.path.join(download_folder, relative_folder_path)

        os.makedirs(save_folder, exist_ok=True)  # 로컬 저장 폴더 생성
        save_folder_path = os.path.join(save_folder, file_name)
        self.logger.debug(f"{self.ftp_host} save_folder_path: {save_folder_path}")

        buffer = io.BytesIO()
        try:
            start_time = time.time()
            # FTP 서버 내 SUBSAMPLING_DATA 폴더에서 파일 다운로드
            remote_path = f"/{self.remote_folder}{fn}"
            self.logger.debug(f"Try {remote_path} download")
            ret = self.ftp.retrbinary(
                f"RETR {remote_path}", buffer.write, blocksize=self.buffer_size
            )
            end_time = time.time()
            if buffer.tell() != 0:
                self.logger.debug(
                    f"Download Performance: {(end_time - start_time):.2f} seconds, "
                    f"{buffer.tell() / (end_time - start_time) / 1024:.2f} Kbytes/sec"
                )
        except Exception as e:
            self.logger.error(f"{self.ftp_host} retrbinary error: {e}")
            raise

        if buffer.tell() == 0:
            self.logger.debug(f"{self.ftp_host} -- File not found {fn}")
            raise FileNotFoundError(f"File not found {fn}")

        self.logger.debug(ret)
        buffer.seek(0)

        # 로컬 파일에 다운로드 받은 데이터를 기록
        with open(save_folder_path, "wb") as file:
            file.write(buffer.read())

        # 파일명이 CSV인 경우 바로 반환
        if save_folder_path.endswith(".csv"):
            return
        if self.data_parsing:
            # FTP를 받자마자 CSV 변환
            try:
                # 데이터 파싱 후 CSV 파일로 저장
                buffer.seek(0)
                parsed_data = buffer.read()
                df = parse_input_data(parsed_data, self.remote_range, DATA_BITS)

                if df.empty:
                    self.logger.critical(
                        f"{self.ftp_host} Data parsing failed for file {fn}"
                    )
                    return

                output_file = os.path.join(
                    save_folder,
                    os.path.splitext(os.path.basename(save_folder_path))[0] + ".csv",
                )

                self.write_file(df, output_file)
            except Exception as err:
                self.logger.critical(f"parse_input_data error {repr(err)}")

    def write_file(self, df: pd.DataFrame, output_file: str) -> None:
        try:
            df.to_csv(output_file, index=False)
        except PermissionError:
            self.logger.critical(
                f"{self.ftp_host} File {output_file} is in use and cannot be saved."
            )
        except Exception as err:
            self.logger.critical(
                f"{self.ftp_host} An unexpected error occurred while writing file: {err}"
            )
        else:
            self.logger.debug(
                f"{self.ftp_host} File {output_file} has been successfully created."
            )

    def retrieve_directory_contents(self, directory):
        contents = []
        try:
            # MLSD 명령어로 디렉토리 내 파일 및 디렉토리 정보를 가져옴
            self.ftp.retrlines("MLSD", contents.append)
        except Exception as e:
            self.logger.error(f"{self.ftp_host} MLSD error: {e}")
            raise Exception(e)
        self.logger.debug(f"{self.ftp_host} MLSD: {len(contents)} items received")
        file_list = []
        for file_info in contents:
            # 공백으로 구분하여 마지막 토큰(파일명)을 추출
            data = file_info.split(" ")
            file_list.append(data[-1])
        return file_list
