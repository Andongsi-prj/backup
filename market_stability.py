from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text  # ✅ text 추가
import os
import pandas as pd

# ✅ MySQL 연결 설정
engine = create_engine("mysql+pymysql://analysis_user:andong1234@192.168.0.163:3306/analysis")
Session = sessionmaker(bind=engine)
session = Session()

# ✅ 폴더 경로 설정
folder_path = r"C:\Users\Admin\Desktop\my-project"

# ✅ "수급안정화지수"가 포함된 파일 목록 가져오기
file_list = [f for f in os.listdir(folder_path) if "수급안정화지수" in f and f.endswith('.csv')]

if not file_list:
    print("🚨 '수급안정화지수' 파일을 찾을 수 없습니다.")
else:
    print(f"📂 {len(file_list)}개의 '수급안정화지수' 파일을 찾았습니다: {file_list}")

    # ✅ 원자재별 컬럼명 매핑 (수급 안정화 지수 컬럼)
    rename_dict = {
        '알루미늄': 'aluminum_supply_stability',
        '크롬': 'chrome_supply_stability',
        '규소': 'silicon_supply_stability'
    }

    # ✅ 모든 파일 읽기 및 데이터프레임 리스트 저장
    all_data = []

    for file_name in file_list:
        file_path = os.path.join(folder_path, file_name)
        print(f"📄 파일 읽기: {file_name}")

        # ✅ CSV 파일 읽기 (A3: 기준월, B3: 수급 안정화 지수 값)
        df = pd.read_csv(file_path, skiprows=3, usecols=[0, 1], names=['record_date', 'supply_index'], encoding='cp949')

        # ✅ record_date 변환 개선 (YYYYMM → YYYY-MM-01)
        df['record_date'] = df['record_date'].astype(str).str.zfill(6)  # 202310 → '202310'
        df['record_date'] = df['record_date'].str[:4] + '-' + df['record_date'].str[4:] + '-01'  # '2023-10-01'

        # ✅ 파일명에서 원자재 이름 추출
        material_name = file_name.split('-')[0].split('_')[-1]  # 예: '알루미늄', '규소', '크롬'

        # ✅ 컬럼명 변경
        if material_name in rename_dict:
            df = df.rename(columns={'supply_index': rename_dict[material_name]})
            df = df[['record_date', rename_dict[material_name]]]
            all_data.append(df)
        else:
            print(f"🚨 파일명에서 원자재 이름을 찾을 수 없음: {file_name}")

    # ✅ 데이터 병합 (record_date 기준)
    if all_data:
        final_df = all_data[0]
        for df in all_data[1:]:
            final_df = pd.merge(final_df, df, on='record_date', how='outer')

        # ✅ 최종 데이터 확인
        print("🧐 최종 데이터 개수:", len(final_df))
        print(final_df.head())

        # ✅ MySQL에 저장 (중복되면 UPDATE)
        data_list = final_df.to_dict(orient='records')

        for data in data_list:
            sql = text("""
            INSERT INTO market_stable (record_date, aluminum_supply_stability, chrome_supply_stability, silicon_supply_stability)
            VALUES (:record_date, :aluminum_supply_stability, :chrome_supply_stability, :silicon_supply_stability)
            ON DUPLICATE KEY UPDATE
                aluminum_supply_stability = VALUES(aluminum_supply_stability),
                chrome_supply_stability = VALUES(chrome_supply_stability),
                silicon_supply_stability = VALUES(silicon_supply_stability);
            """)  

            session.execute(sql, data)  # ✅ SQL 실행

        session.commit()
        session.close()

        print(f"✅ {len(final_df)}건의 데이터가 market_stable 테이블에 업데이트되었습니다.")
    else:
        print("🚨 저장할 데이터가 없습니다.")
