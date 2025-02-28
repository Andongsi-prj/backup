import requests
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy import create_engine, text

# MySQL 연결 설정
engine = create_engine("mysql+pymysql://analysis_user:andong1234@192.168.0.163:3306/analysis")

# 1. 종목 코드, 이름, 현재가 정보 가져오기
def get_stock_data():
    url = "https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no=304"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    
    stock_data = []
    table = soup.find("table", {"class": "type_5"})  # 해당 HTML 테이블 찾기
    rows = table.find_all("tr")[2:]  # 테이블의 첫 번째 두 행을 제외한 나머지 데이터 추출
    
    for row in rows:
        cols = row.find_all("td")
        if len(cols) > 1:
            stock_name = cols[0].get_text(strip=True)  # 회사명
            stock_code = cols[0].find("a")["href"].split("code=")[-1]  # 종목 코드
            current_price = cols[1].get_text(strip=True).replace(",", "")  # 현재가
            stock_data.append((stock_name, stock_code, current_price))
    
    return stock_data

# 2. 주식 데이터 DB에 업데이트
def update_stock_prices():
    stock_data = get_stock_data()  # 종목 데이터 가져오기
    current_date = datetime.now()

    for stock in stock_data:
        stock_name = stock[0]
        stock_code = stock[1]
        current_price = stock[2]
        
        # 쿼리 작성 - 특정 종목에 대해 현재가를 업데이트
        query = f"""
            UPDATE stage_stock
            SET stock_price = {current_price}
            WHERE stock_code = '{stock_code}' AND stock_date = '{current_date.strftime('%Y-%m-%d')}'
        """
        
        # 쿼리 실행 및 디버깅 로그
        print(f"Executing query: {query}")
        
        with engine.connect() as connection:
            result = connection.execute(text(query))  # query를 text로 감싸서 실행
            connection.commit()  # 트랜잭션 커밋
            print(f"Rows affected: {result.rowcount}")  # 영향을 받은 행 수 출력
            if result.rowcount > 0:
                print(f"주식명: {stock_name}, 종목 코드: {stock_code}, 현재가: {current_price}, 날짜: {current_date.strftime('%Y-%m-%d')} - 업데이트 완료")
            else:
                print(f"주식명: {stock_name}, 종목 코드: {stock_code} - 해당 데이터가 없어서 업데이트되지 않았습니다.")

# 3. 실행
if __name__ == "__main__":
    update_stock_prices()
