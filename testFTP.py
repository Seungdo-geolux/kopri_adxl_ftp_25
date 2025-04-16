import io
import time
from datetime import datetime
from ftplib import FTP

from loguru import logger


def test():
    logger.info("start")
    for pasv_mode in [
        False,
        True,
    ]:
        logger.info(f"TEST  {pasv_mode=} -----")
        ftp = FTP("192.168.0.201", timeout=15.0)

        try:
            ftp.login(user="Kopri", passwd="KopriW5500")
            ftp.set_pasv(pasv_mode)

            current_directory = ftp.pwd()
            logger.debug(f"현재 작업 디렉토리: {current_directory}")

            # 루트 디렉토리 MLSD 테스트
            root_dir = []
            ftp.retrlines("MLSD", root_dir.append)
            logger.info("루트 디렉토리(MLSD):")
            for item in root_dir:
                logger.debug(item)
            logger.info("루트 MLSD LIST SUCCESS")

            TEST_FOLDER = root_dir[-1].split()[-1]
            logger.info(f"MLSD PATH Folder : {TEST_FOLDER}")
            mlsdpath_listing = []
            ftp.retrlines(f"MLSD {TEST_FOLDER}", mlsdpath_listing.append)
            for item in mlsdpath_listing:
                logger.debug(item)
            logger.info("MLSD PATH:'{TEST_FOLDER}' LIST SUCCESS")

            # 특정 디렉토리로 이동
            ftp.cwd(TEST_FOLDER)
            logger.info("디렉토리 '{TEST_FOLDER}' 이동 (CWD) 성공")

            # ===== FTP 명령어 동작 확인 테스트 =====
            logger.info("=== FTP 명령어 테스트 시작: MLSD, LIST, NLST, MDTM, MFMT ===")

            # 1. MLSD 명령어 테스트
            mlsd_listing = []
            ftp.retrlines("MLSD", mlsd_listing.append)
            logger.debug("MLSD listing:")
            for item in mlsd_listing:
                logger.debug(item)
            logger.info("MLSD 명령어 테스트 완료")

            # 2. LIST 명령어 테스트
            list_listing = []
            ftp.retrlines("LIST", list_listing.append)
            logger.debug("LIST listing:")
            for item in list_listing:
                logger.debug(item)
            logger.info("LIST 명령어 테스트 완료")

            # 3. NLST 명령어 테스트
            try:
                nlst_listing = ftp.nlst()
                logger.debug("NLST listing:")
                for item in nlst_listing:
                    logger.debug(item)
                logger.info("NLST 명령어 테스트 완료")
            except Exception as e:
                logger.error(f"NLST 명령어 실패: {repr(e)}")
                nlst_listing = []

            # 4. MDTM 및 MFMT 명령어 테스트 (파일이 있을 경우)
            if nlst_listing:
                test_file = nlst_listing[
                    0
                ]  # 첫 번째 파일(또는 항목)을 테스트 대상으로 선택
                try:
                    mdtm_response = ftp.sendcmd("MDTM " + test_file)
                    logger.debug(f"MDTM response for {test_file}: {mdtm_response}")
                    logger.info("MDTM 명령어 테스트 완료")
                except Exception as e:
                    logger.error(f"MDTM 명령어 실패 ({test_file}): {repr(e)}")
                try:
                    # MFMT 명령어: 새 수정 시간을 설정 (형식: YYYYMMDDhhmmss)
                    new_time = datetime.now().strftime("%Y%m%d%H%M%S")
                    mfmt_response = ftp.sendcmd(f"MFMT {new_time} {test_file}")
                    logger.debug(f"MFMT response for {test_file}: {mfmt_response}")
                    logger.info("MFMT 명령어 테스트 완료")
                except Exception as e:
                    logger.error(f"MFMT 명령어 실패 ({test_file}): {repr(e)}")
            else:
                logger.warning("MDTM/MFMT 테스트를 위한 파일이 존재하지 않습니다.")

            # ===== 파일 다운로드 테스트 =====
            # mlsd_listing를 이용하여 파일정보를 파싱하여 다운로드
            if not list_listing:
                logger.info(f"{len(mlsd_listing)} Files found (다운로드 미실행)")
            else:
                logger.info(f"{len(mlsd_listing)} Files (다운로드 실행중)")
                download_start_time = time.time()
                for item in list_listing:
                    fileinfo = item.split()
                    filename = fileinfo[-1]
                    buffer = io.BytesIO()
                    ftp.retrbinary(f"RETR {filename}", buffer.write)
                    # 파일 크기 비교 (파일 크기는 MLSD의 5번째 요소라고 가정)
                    if buffer.tell() == int(fileinfo[4]):
                        logger.debug(
                            f"다운로드 성공: {filename} (크기 = {buffer.tell()})"
                        )
                logger.info(f"다운로드 Time = {time.time()-download_start_time}")

            # 상위 디렉토리로 이동 (CDUP)
            ftp.sendcmd("CDUP")
            new_root = []
            ftp.retrlines("MLSD", new_root.append)
            if root_dir == new_root:
                logger.info("CDUP 명령어 성공")
            else:
                logger.error("CDUP 명령어 후 디렉토리 목록 불일치")

            logger.info(f"TEST {pasv_mode=} Success")
        except Exception as err:
            logger.error(f"{repr(err)}")

        ftp.quit()
        time.sleep(1.5)


def main():
    for i in range(3):
        print(f"TEST Try {i+1}")
        test()
        logger.info(f"--------- {i+1}th TEST Complete")
        time.sleep(2)


if __name__ == "__main__":
    import sys

    logger.configure(handlers=[{"sink": sys.stdout, "level": "WARNING"}])
    main()
