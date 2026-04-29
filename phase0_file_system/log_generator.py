import time
import csv
import random
import os
import shutil
from datetime import datetime
from faker import Faker

fake = Faker()
FILE_PATH = "data/user_log.csv"
MAX_FILE_SIZE = 10 * 1024  # 💥 10KB (순식간에 쪼개짐)

def rotate_log_file():
    if os.path.exists(FILE_PATH) and os.path.getsize(FILE_PATH) > MAX_FILE_SIZE:
        timestamp = datetime.now().strftime("%H%M%S")
        rotated_name = f"data/user_log_{timestamp}.csv"
        
        # 1. 기존 파일 이름 변경 (Rotation)
        shutil.move(FILE_PATH, rotated_name)
        print(f"\n🔄 [System] 파일 교체됨! ({FILE_PATH} -> {rotated_name})\n")
        
        # 2. 새 파일 생성 (빈 파일)
        with open(FILE_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['event_time', 'user_id', 'event_type', 'product_id', 'price'])
            writer.writeheader()

def generate_log():
    event_types = ['click', 'view', 'purchase']
    current_event = random.choice(event_types)
    return {
        'event_time': datetime.now().isoformat(),
        'user_id': fake.uuid4(),
        'event_type': current_event,
        'product_id': fake.ean8() if current_event == 'purchase' else None,
        'price': round(random.uniform(10.0, 500.0), 2) if current_event == 'purchase' else 0
    }

def main():
    print(f"🚀 Generator 시작 (10KB마다 파일 교체)")
    
    # 초기 파일 생성
    if not os.path.exists(FILE_PATH):
        with open(FILE_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['event_time', 'user_id', 'event_type', 'product_id', 'price'])
            writer.writeheader()

    while True:
        rotate_log_file() # 파일 크기 체크

        log_data = generate_log()
        with open(FILE_PATH, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=log_data.keys())
            writer.writerow(log_data)
        
        time.sleep(0.05) # 너무 빠르면 로그 못보니까 살짝 대기

if __name__ == "__main__":
    main()