from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pytz
import pendulum  # pendulum을 사용하여 시간대 처리

# MySQL 연결 설정
engine = create_engine("mysql+pymysql://analysis_user:andong1234@192.168.0.163:3306/analysis")

# KST 타임존 설정
kst = pytz.timezone('Asia/Seoul')

# 1. 종목 코드 수집 함수
def get_stock_codes():
    url = "https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no=304"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    stocks = []
    table = soup.find("table", {"class": "type_5"})
    rows = table.find_all("tr")[2:]  # 테이블의 첫 번째 두 행을 제외한 나머지 데이터 추출

    for row in rows:
        cols = row.find_all("td")
        if len(cols) > 1:
            stock_name = cols[0].get_text(strip=True)  # 회사명
            stock_code = cols[0].find("a")["href"].split("code=")[-1]  # 종목 코드
            current_price = cols[1].get_text(strip=True).replace(",", "")  # 현재가
            stocks.append((stock_name, stock_code, current_price))
    
    return stocks

# 2. 주식 가격 업데이트 함수
def update_stock_prices():
    stock_data = get_stock_codes()
    current_time = datetime.now(kst)  # 현재 시간을 KST로 설정
    current_date = current_time.date()

    for stock in stock_data:
        stock_name = stock[0]
        stock_code = stock[1]
        current_price = stock[2]
        
        query = f"""
            SELECT * FROM stage_stock 
            WHERE stock_code = '{stock_code}' AND stock_date = '{current_date}'
        """
        existing_data = pd.read_sql(query, con=engine)

        if not existing_data.empty:
            # 데이터가 이미 존재하면 가격을 업데이트
            update_query = f"""
                UPDATE stage_stock 
                SET stock_price = {current_price} 
                WHERE stock_code = '{stock_code}' AND stock_date = '{current_date}'
            """
            engine.execute(update_query)
            print(f"주식명: {stock_name}, 종목 코드: {stock_code}, 현재가: {current_price}, 날짜: {current_date} - 업데이트 완료")
        else:
            # 데이터가 없으면 새로 삽입
            insert_query = f"""
                INSERT INTO stage_stock (stock_name, stock_code, stock_price, stock_date) 
                VALUES ('{stock_name}', '{stock_code}', {current_price}, '{current_date}')
            """
            engine.execute(insert_query)
            print(f"주식명: {stock_name}, 종목 코드: {stock_code}, 현재가: {current_price}, 날짜: {current_date} - 삽입 완료")

# 3. 종가 업데이트 함수 (16시 기준으로 실행)
def update_close_price():
    current_time = datetime.now(kst)
    current_date = current_time.date()

    query = f"""
        SELECT * FROM stage_stock
        WHERE stock_date = '{current_date}'
    """
    stock_data = pd.read_sql(query, con=engine)
    
    if not stock_data.empty:
        update_query = f"""
            UPDATE stage_stock 
            SET stock_price = stock_price 
            WHERE stock_date = '{current_date}'
        """
        engine.execute(update_query)
        print(f"주식 가격이 종가로 업데이트되었습니다. 날짜: {current_date}")
    else:
        print(f"오늘 데이터가 없습니다. 날짜: {current_date}")

# 기본 인자 설정
default_args = {
    'owner': 'andong',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 2, 13, 9, 0, tz='Asia/Seoul'),  # 9시부터 시작 (KST 기준)
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'timezone': 'Asia/Seoul'  # KST 시간대 설정
}

# DAG 정의
with DAG(
    'real_time_stock_pipeline',
    default_args=default_args,
    description='9시부터 16시까지 실시간 가격 업데이트 및 16시에 종가 기록',
    schedule_interval='*/10 9-16 * * 1-5',  # 매주 월~금, 9시~16시 매 10분마다 실행 (UTC 기준, KST 9시부터 16시까지)
    catchup=False,
    tags=['real_time', 'stock'],
) as dag:

    start = PythonOperator(
        task_id='start',
        python_callable=lambda: print("DAG 시작")
    )

    update_real_time_price_task = PythonOperator(
        task_id='update_real_time_price',
        python_callable=update_stock_prices
    )

    update_close_price_task = PythonOperator(
        task_id='update_close_price',
        python_callable=update_close_price,
        trigger_rule='all_done'
    )

    end = PythonOperator(
        task_id='end',
        python_callable=lambda: print("DAG 종료")
    )

    start >> update_real_time_price_task >> update_close_price_task >> end
