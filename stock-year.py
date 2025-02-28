import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# MySQL 연결 설정
engine = create_engine("mysql+pymysql://analysis_user:andong1234@192.168.0.163:3306/analysis")

# 1. 종목 코드 수집 함수
def get_stock_codes():
    url = "https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no=304"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    stocks = []
    table = soup.find("table", {"class": "type_5"})
    rows = table.find_all("tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) > 1:
            stock_name = cols[0].get_text(strip=True)
            stock_code = cols[0].find("a")["href"].split("code=")[-1]
            stocks.append((stock_name, stock_code))
    
    return pd.DataFrame(stocks, columns=["stock_name", "stock_code"])

# 2. 2023년 1월부터 현재까지 주가 데이터 수집 함수
def get_stock_history(code):
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    
    url = f"https://api.finance.naver.com/siseJson.naver?symbol={code}&requestType=1&startTime={start_date.strftime('%Y%m%d')}&endTime={end_date.strftime('%Y%m%d')}&timeframe=day"
    response = requests.get(url)
    data_str = response.text.replace('\'', '"').strip()
    
    try:
        data_str = data_str[data_str.index('['):].strip()
        df = pd.read_json(data_str).iloc[1:]
        df.columns = ['stock_date', 'open_price', 'high_price', 'low_price', 'stock_price', 'volume', 'foreign_rate']
        df['stock_date'] = pd.to_datetime(df['stock_date'], format='%Y%m%d')
        return df[['stock_date', 'stock_price']]
    except ValueError:
        print(f"Failed to fetch data for stock code: {code}")
        return pd.DataFrame()

# 3. MySQL에 데이터 저장 함수 (2023년부터 현재까지 데이터 삽입)
def update_stock_data():
    all_stock_data = []
    df_stocks = get_stock_codes()  # 종목 코드 수집
    
    for index, row in df_stocks.iterrows():
        stock_code = row["stock_code"]
        stock_name = row["stock_name"]
        stock_data = get_stock_history(stock_code)
        
        if not stock_data.empty:
            stock_data['stock_name'] = stock_name
            stock_data['stock_code'] = stock_code
            all_stock_data.append(stock_data)
    
    if all_stock_data:
        final_df = pd.concat(all_stock_data, ignore_index=True)
        final_df.columns = ['stock_date', 'stock_price', 'stock_name', 'stock_code']
        final_df.to_sql('stage_stock', con=engine, if_exists='append', index=False)
        print(f"{len(final_df)}건의 데이터를 stage_stock 테이블에 추가했습니다.")
    else:
        print("데이터가 없습니다.")

# 4. 실행
if __name__ == "__main__":
    update_stock_data()
