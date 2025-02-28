import os
import pandas as pd
from sqlalchemy import create_engine
import pymysql

# MySQL 연결 설정
engine = create_engine("mysql+pymysql://analysis_user:andong1234@192.168.0.163:3306/analysis")

# 파일 경로 설정
folder_path = r"C:\Users\Admin\Desktop\my-project"  # 바탕화면 폴더 경로
file_list = os.listdir(folder_path)

# 모든 CSV 파일 처리
all_data = []

for file_name in file_list:
    if file_name.endswith('.csv'):
        file_path = os.path.join(folder_path, file_name)
        
        # CSV 파일 읽기 (A4부터 시작, 인코딩 설정)
        df = pd.read_csv(file_path, skiprows=3, usecols=[0, 1], names=['one_date', 'one_price'], encoding='cp949')
        
        # 기준일을 날짜 형식으로 변환 (YYYYMMDD → YYYY-MM-DD), 잘못된 값은 NaT로 변환
        df['one_date'] = pd.to_datetime(df['one_date'], format='%Y%m%d', errors='coerce')
        
        # NaT 값 제거 (잘못된 날짜 제거)
        df = df.dropna(subset=['one_date'])
        
        # 파일명에서 원자재 이름 추출
        one_name = file_name.split('_')[2]  # '크롬', '알루미늄', '철', '유연탄' , '규소 ' 추출
        df['one_name'] = one_name
        
        all_data.append(df)

# 데이터 합치기
final_df = pd.concat(all_data, ignore_index=True)

# MySQL에 데이터 저장
final_df.to_sql('stage_one', con=engine, if_exists='append', index=False)
print(f"{len(final_df)}건의 데이터가 stage_one 테이블에 추가되었습니다.")
