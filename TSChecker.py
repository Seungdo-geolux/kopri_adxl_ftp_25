import datetime
import glob
import os

import numpy as np
import pandas as pd


def extract_time(filename):
    """
    파일명에서 시간(HHMM)을 추출합니다.
    예상 파일명 패턴: prefix_날짜_HHMM_20.csv (예: KOA_20250405_0000_20.csv)
    """
    basename = os.path.basename(filename)
    parts = basename.split("_")
    if len(parts) < 4:
        print(f"파일명 형식 오류: {basename}")
        return None
    time_str = parts[2]  # 인덱스 2에 HHMM이 있다고 가정
    try:
        return datetime.datetime.strptime(time_str, "%H%M")
    except ValueError as e:
        print(f"{basename} 파일의 시간 파싱 오류: {e}")
        return None


# CSV 파일들이 저장된 폴더 경로
folder_base = r"C:\myWork\python\kopri_adxl_ftp_25\Saved_Data_KOA"
folder_date = "20250421"
folder_path = os.path.join(folder_base, folder_date)
file_pattern = os.path.join(folder_path, "*_20.csv")
csv_files = sorted(glob.glob(file_pattern))

print(f"{folder_date} 폴더에서 총 {len(csv_files)}개의 CSV 파일을 찾았습니다.")

# 기준 시작 시간 (HHMM 형식)
start_datetime = datetime.datetime.strptime("0000", "%H%M")

if not csv_files:
    print(f"{folder_path}에 '_20.csv' 형식의 파일이 없습니다.")
else:
    for file in csv_files:
        # print(file)
        file_time = extract_time(file)
        if file_time is None:
            print(f"시간 추출 오류로 파일을 건너뜁니다: {file}")
            continue

        if start_datetime != file_time:
            print(
                f"예상 시작 시각 {start_datetime.strftime('%H%M')}에 해당하는 파일이 없습니다."
            )
            # 파일 시간 기준으로 시작 시각 갱신 (2분 추가)
            start_datetime = file_time + datetime.timedelta(minutes=2)
        else:
            start_datetime = start_datetime + datetime.timedelta(minutes=2)

        try:
            df = pd.read_csv(file)
            # 'Timestamp' 열이 있는지 확인
            if "Timestamp" not in df.columns:
                print(f"{file} - Timestamp 열이 없습니다.")
                continue

            timestamps = df["Timestamp"].values

            # 인접 행 간 타임스탬프 차이를 계산 (정수 값, 차이는 50이어야 함)
            diffs = np.diff(timestamps)

            # 음수 차이에 대해 1000을 더해 wrap-around 처리
            for i, value in enumerate(diffs):
                if value < 0:
                    diffs[i] = 1000 + value

            # 모든 차이가 정확히 50인지 확인
            if np.all(diffs == 50):
                print(
                    f"{os.path.basename(file)} - 타임스탬프 간격이 일정합니다. {len(timestamps)} lines"
                )
                # pass
            else:
                print(
                    f"{os.path.basename(file)} - 타임스탬프 간격에 불규칙성이 발견되었습니다. {len(timestamps)} lines"
                )
                # print("차이값들:", diffs)
                # for data in diffs:
                #     print(data)

        except Exception as e:
            print(f"{os.path.basename(file)} - 파일 처리 중 오류 발생: {e}")
