import json
import os
import sys

from jsonschema import validate
from loguru import logger


class mySettings:

    def __init__(
        self, defaults: dict, file_name="settings.json", folder_name="./"
    ) -> None:
        # 파일 경로 설정
        self.file_path = os.path.join(folder_name, file_name)
        self.defaults = defaults
        # 초기 로깅을 위해 기본 logger 사용 (설정값을 읽기 전)
        self.logger = logger
        # 설정 파일 읽기 (파일이 없으면 기본값을 기록함)
        self.settings = self.read()
        # 설정에 따른 logger 재설정
        self.logger = self._logger_init()

    def _logger_init(self):
        # 설정값에서 logger 폴더와 파일명을 가져옴
        logger_folder = self.settings.get("logger", {}).get("folder", "./log")
        logger_filename = self.settings.get("logger", {}).get("filename", "app.log")
        logger_console = self.settings.get("logger", {}).get("console", True)
        logger_level = self.settings.get("logger", {}).get("level", "DEBUG")
        logger_path = os.path.join(logger_folder, logger_filename)

        logger.remove()
        if logger_console:
            logger.add(sys.stdout, level=logger_level)
        else:
            self.logger.warning(f"{logger_console =}")

        logger.add(logger_path, rotation="10 MB", level=logger_level)
        logger.info("Logger initialized")
        return logger

    def _logger(self, message):
        # logger가 존재하면 debug 레벨로 메시지 기록, 없으면 print 사용
        if hasattr(self, "logger"):
            self.logger.debug(message)
        else:
            print(message)

    def read(self):
        try:
            # UTF-8 인코딩으로 설정 파일 읽기
            with open(self.file_path, "r", encoding="utf-8") as file:
                data = json.load(file)
            self._logger(data)
            return data
        except FileNotFoundError as err:
            # 파일이 없으면 기본값을 파일에 기록하고 기본값 반환
            self._logger(f"{repr(err)}")
            self.write(self.defaults)
            return self.defaults

    def write(self, data: dict) -> None:
        try:
            # UTF-8 인코딩으로 설정 데이터를 파일에 기록
            with open(self.file_path, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=4)
        except Exception as err:
            self._logger(err)
            raise err

    def save(self):
        self.write(self.settings)

    def get_value(self, key: str) -> any:
        try:
            return self.settings[key]
        except KeyError as err:
            self._logger(err)
            raise KeyError(err)

    def get_logger(self):
        return self.logger

    def validate(self, schema):
        try:
            validate(instance=self.settings, schema=schema)
        except Exception as err:
            self._logger(repr(err))
            return False
        return True
