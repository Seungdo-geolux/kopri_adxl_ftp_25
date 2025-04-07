import datetime
import os
import pickle
import threading
import time
from xmlrpc.client import Boolean, boolean

from myFTPClient import myFTP
from mySettings import mySettings

COMPLETE_SAVE = True


def generate_filenames_from_UTC_decrease_minute(
    logger, stop_time, prefix, file_duration
):
    # 현재 UTC 시간을 가져옴
    start_time = datetime.datetime.now(datetime.timezone.utc)
    # 현재 시간이 stop_time보다 이후여야 함
    if start_time < stop_time:
        logger.error("Error: Current UTC time is earlier than stop_time")
        return

    # file_duration만큼 시간을 빼서 시작 시간 설정
    current_time = start_time - datetime.timedelta(minutes=file_duration)

    # current_time의 분이 file_duration의 배수가 될 때까지 1분씩 감소
    while True:
        if current_time.minute % file_duration == 0:
            break
        current_time -= datetime.timedelta(minutes=1)

    # 파일 이름 생성 루프
    while True:
        # 60분 이상 차이나고, 분이 0인 경우 Solar 파일 이름 생성
        if current_time <= start_time - datetime.timedelta(minutes=60):
            if current_time.minute == 0:
                folder_name = current_time.strftime("%Y%m%d")
                file_name = f"/{folder_name}/{prefix}_{current_time.strftime('%y%m%d_%H')}_solar.csv"
                yield file_name

        # 일반 Date 파일 이름 생성
        folder_name = current_time.strftime("%Y%m%d")
        file_name = (
            f"/{folder_name}/{prefix}_{current_time.strftime('%y%m%d_%H%M')}_20.dat"
        )
        yield file_name

        # file_duration 만큼 시간 감소
        current_time -= datetime.timedelta(minutes=file_duration)
        # 만약 current_time가 stop_time보다 이전이면 종료
        if current_time <= stop_time:
            return


def file_exist(file_path, downfolder):
    # 예시: file_path "/20250319/KOA_250319_..."; 선행 슬래시 제거 후 안전하게 경로 결합
    folder_path, file_name = os.path.split(file_path)
    relative_folder_path = folder_path.lstrip("/\\")  # 선행 슬래시 제거
    save_folder = os.path.join(downfolder, relative_folder_path)

    # 저장 폴더가 없으면 생성하고 파일이 없다고 판단
    if not os.path.exists(save_folder):
        os.makedirs(save_folder, exist_ok=True)
        return False

    save_file_path = os.path.join(save_folder, file_name)
    return os.path.exists(save_file_path)


def _delay(delay_time, event):
    # delay_time 만큼 1초 단위로 지연하며, 이벤트가 설정되면 종료
    delay = int(delay_time)
    for _ in range(delay):
        time.sleep(1)
        if event.is_set():
            break


def ftp_task(logger, device, stop_event, error_event, config_save):
    # 시작 시간 문자열을 datetime 객체로 변환 (오류 발생 시 로깅 후 종료)
    try:
        local_time = datetime.datetime.strptime(
            device["start_time"], "%Y-%m-%d %H:%M:%S"
        )
    except Exception as err:
        logger.error(f"Time format error: {err}")
        return

    # UTC 타임존 적용
    target_date = local_time.replace(tzinfo=datetime.timezone.utc)

    ftp_host = device["ftp_host"]
    retry_delay = device["retry_delay"]
    download_folder = device["download_folder"]
    file_duration = device["file_duration"]
    remote_prefix = device["remote_prefix"]

    logger.info(f"FTP task started for ftp_host={ftp_host}")

    # 다운로드 폴더가 없으면 생성
    if not os.path.exists(download_folder):
        os.makedirs(download_folder, exist_ok=True)
        logger.info(f"{ftp_host}: Download folder created at {download_folder}")

    # FTP 작업 루프
    while not stop_event.is_set():

        try:
            with open("filenotfound_list.pkl", "rb") as f:
                filenotfound_list = pickle.load(f)
        except FileNotFoundError:
            filenotfound_list = []

        downloader = myFTP(logger, device)
        # FTP 연결 테스트 실패 시 재시도
        if not downloader.connection_test():
            logger.info(f"{ftp_host}: PING failed at {datetime.datetime.now()}")
            if retry_delay != 0:
                _delay(retry_delay, stop_event)
                continue
            else:
                break
        try:
            start_time = datetime.datetime.now(datetime.timezone.utc)
            # 파일 이름 생성기 초기화
            filename_generator = generate_filenames_from_UTC_decrease_minute(
                logger, target_date, remote_prefix, file_duration
            )
            downloader.connect()
            logger.info(f"{ftp_host}: FTP connected -------------------- ")

            while not stop_event.is_set():
                try:
                    filename = next(filename_generator)
                    # 파일이 없다는 리스트에 있으면 넘어감
                    if filename in filenotfound_list:
                        logger.debug(f"File name in filenotfound_list {filename}")
                        continue
                    # 파일이 이미 존재하면 넘어감
                    if file_exist(filename, download_folder):
                        logger.debug(f"File exist '{filename}'")
                        continue
                except StopIteration:
                    if COMPLETE_SAVE:
                        restart_time = start_time - datetime.timedelta(
                            minutes=file_duration
                        )  # 현재 시각에서 한 주기 전 파일 다운로드
                        formatted_restart_time = restart_time.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        device["start_time"] = formatted_restart_time
                        logger.debug(
                            f"{ftp_host}: RESET Taget_date: {formatted_restart_time}"
                        )
                        target_date = restart_time
                        config_save()

                        with open("filenotfound_list.pkl", "wb") as f:
                            pickle.dump([], f)
                    break

                try:
                    # FTP에서 파일 다운로드
                    downloader.download_files_from_ftp(filename, download_folder)
                    logger.info(f"{ftp_host}: {filename} successfully transferred")
                except ConnectionResetError as e:
                    logger.error(
                        f"{ftp_host}: ConnectionResetError for {filename}: {e}"
                    )
                    raise
                except ConnectionRefusedError as e:
                    logger.error(
                        f"{ftp_host}: ConnectionRefusedError for {filename}: {e}"
                    )
                    raise
                except Exception as e:
                    logger.error(f"{ftp_host}: Download error for {filename}: {e}")
                    filenotfound_list.append(filename)
                    continue

            downloader.disconnect()
            logger.info(f"{ftp_host}: FTP disconnected at {datetime.datetime.now()}")
            del downloader
            with open("filenotfound_list.pkl", "wb") as f:
                pickle.dump(filenotfound_list, f)

        except Exception as err:
            logger.error(f"{ftp_host}: Exception occurred: {err}")
            downloader.disconnect()
            del downloader
            error_event.set()
            logger.debug(f"{ftp_host}: error_event_set")

        finally:
            logger.debug(f"{ftp_host}: finally Excuted")
            if retry_delay != 0:
                _delay(retry_delay, stop_event)
                continue
            else:
                break

    logger.info(f"FTP task stopped for ftp_host={ftp_host}")


def main():
    # 기본 설정값 정의
    defaults = {
        "logger": {
            "folder": "./log",
            "filename": "KOPRI_ADXL_FTP.log",
            "console": True,
            "level": "DEBUG",
        },
        "name": "KOPRI ADXL FTP SETTINGS",
        "version": "20250319",
        "devices": [
            {
                "ftp_host": "192.168.0.200",
                "ftp_user": "Kopri",
                "ftp_password": "KopriW5500",
                "download_folder": "./Saved_Data_KOA",
                "start_time": "2025-03-19 00:00:00",
                "retry_delay": 300,
                "buffer_size": 1024,
                "file_duration": 2,
                "passice_mode": False,
                "remote_folder": "SUBSAMPLING_DATA",
                "remote_prefix": "KOA",
                "remote_range": 4,
                "data_parsing": False,
            },
            {
                "ftp_host": "192.168.0.201",
                "ftp_user": "Kopri",
                "ftp_password": "KopriW5500",
                "download_folder": "./Saved_Data_KOB",
                "start_time": "2025-03-19 00:00:00",
                "retry_delay": 300,
                "buffer_size": 1024,
                "file_duration": 2,
                "passice_mode": False,
                "remote_folder": "SUBSAMPLING_DATA",
                "remote_prefix": "KOB",
                "remote_range": 4,
                "data_parsing": False,
            },
        ],
    }
    # JSON 스키마 정의 및 설정 검증
    schema = {
        "type": "object",
        "properties": {
            "logger": {
                "type": "object",
                "properties": {
                    "folder": {"type": "string"},
                    "filename": {"type": "string"},
                    "console": {"type": "boolean"},
                    "level": {"type": "string"},
                },
            },
            "name": {"type": "string"},
            "version": {"type": "string"},
            "devices": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "ftp_host": {"type": "string"},
                        "ftp_user": {"type": "string"},
                        "ftp_password": {"type": "string"},
                        "download_folder": {"type": "string"},
                        "start_time": {"type": "string", "format": "date-time"},
                        "retry_delay": {"type": "integer"},
                        "buffer_size": {"type": "integer"},
                        "file_duration": {"type": "integer"},
                        "passive_mode": {"type": "boolean"},
                        "remote_prefix": {"type": "string", "maxLength": 5},
                        "remote_folder": {"type": "string"},
                        "remote_range": {"type": "integer"},
                        "data_parsing": {"type": "boolean"},
                    },
                    "required": [
                        "ftp_host",
                        "ftp_user",
                        "ftp_password",
                        "download_folder",
                        "start_time",
                        "retry_delay",
                        "buffer_size",
                        "file_duration",
                        "passive_mode",
                        "remote_prefix",
                        "remote_folder",
                        "remote_range",
                    ],
                },
            },
        },
        "required": ["name", "version", "devices"],
    }

    config = mySettings(defaults)
    if not config.validate(schema):
        print("Configuration validation failed. check settings.json")
        return
    # logger 인스턴스 생성
    logger = config.get_logger()
    stop_event = threading.Event()
    error_event = threading.Event()

    logger.info("Program started")

    try:
        threads = []
        # 각 디바이스마다 스레드 생성
        for device in config.get_value("devices"):
            logger.info(f"Device configuration: {device}")
            thread = threading.Thread(
                target=ftp_task,
                args=(logger, device, stop_event, error_event, config.save),
            )
            threads.append(thread)
            thread.start()
            logger.info(f"{device['ftp_host']} Thread OK.")

    except Exception as err:
        logger.critical(f"Error starting thread: {err}")

    # 메인 스레드에서 프로그램 종료 조건 감시
    try:
        while True:
            if error_event.is_set():
                stop_event.set()
                logger.error("Program stopped due to an error.")
                break
            if all(not t.is_alive() for t in threads):
                logger.info("All threads have finished. Exiting main loop.")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Program interrupted by user.")
        stop_event.set()

    for t in threads:
        t.join()

    logger.info("PROGRAM STOPPED")


if __name__ == "__main__":
    main()
