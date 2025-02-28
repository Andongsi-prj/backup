import pandas as pd
from sqlalchemy import create_engine

# MySQL 연결 설정
engine = create_engine("mysql+pymysql://analysis_user:andong1234@192.168.0.163:3306/analysis")

# 1. CSV 파일 읽기
df = pd.read_csv("C:\\Users\\Admin\\Desktop\\my-project\\USD_KRW 과거 데이터.csv", encoding='cp949')

# 데이터프레임의 컬럼명 확인
print("컬럼명:", df.columns.tolist())

# 2. 날짜와 종가 컬럼만 선택
df = df[['날짜', '종가']]

# 3. 컬럼 이름 변경 및 데이터 정리
df.columns = ['exchange_date', 'exchange_price']
df['exchange_date'] = pd.to_datetime(df['exchange_date'], errors='coerce')  # 날짜 형식 변환
df['exchange_price'] = df['exchange_price'].str.replace('"', '').str.replace(',', '').astype(float)  # 종가 값 정리

# 4. MySQL 테이블에 데이터 저장 (stage_exchange)
df.to_sql('stage_exchange', con=engine, if_exists='append', index=False)
print(f"{len(df)}건의 데이터를 stage_exchange 테이블에 추가했습니다.")
