import pymysql
from PIL import Image
import io
import os
import random

# MySQL 연결 설정
conn = pymysql.connect(
    host="192.168.0.163",
    port=3306,
    user="root",
    password="andong1234",
    database="manufacture"
)

# 이미지 경로 설정
normal_path = r"C:\Users\Admin\Desktop\data\resized\학습\정상"
defect_path = r"C:\Users\Admin\Desktop\data\resized\학습\불량"

try:
    with conn.cursor() as cursor:
        # 테이블 초기화 및 AUTO_INCREMENT 재설정
        cursor.execute("TRUNCATE TABLE plt_img")
        
        # 정상 및 불량 이미지 파일 리스트 생성
        normal_images = [f for f in os.listdir(normal_path) if os.path.isfile(os.path.join(normal_path, f))]
        defect_images = [f for f in os.listdir(defect_path) if os.path.isfile(os.path.join(defect_path, f))]
        
        # 20:1 비율로 이미지 저장
        normal_idx = 0
        defect_idx = 0
        
        while normal_idx < len(normal_images):
            # 정상 이미지 20개 저장
            for _ in range(20):
                if normal_idx >= len(normal_images):
                    break
                    
                image_path = os.path.join(normal_path, normal_images[normal_idx])
                with open(image_path, "rb") as image_file:
                    binary_image = image_file.read()
                
                cursor.execute("INSERT INTO plt_img (img) VALUES (%s)", (binary_image,))
                print(f"정상 이미지 저장 완료: {normal_images[normal_idx]}")
                normal_idx += 1
            
            # 불량 이미지 1개 저장
            if defect_idx < len(defect_images):
                image_path = os.path.join(defect_path, defect_images[defect_idx])
                with open(image_path, "rb") as image_file:
                    binary_image = image_file.read()
                
                cursor.execute("INSERT INTO plt_img (img) VALUES (%s)", (binary_image,))
                print(f"불량 이미지 저장 완료: {defect_images[defect_idx]}")
                defect_idx += 1

        conn.commit()
        print("모든 이미지가 성공적으로 저장되었습니다.")

except Exception as e:
    print(f"오류 발생: {str(e)}")
finally:
    conn.close()

