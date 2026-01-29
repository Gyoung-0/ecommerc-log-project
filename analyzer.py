# analyzer.py
import csv
import time
import os
from datetime import datetime

FILE_PATH = "data/user_log.csv"

def calculate_revenue():
    if not os.path.exists(FILE_PATH):
        print(f"⚠️ 파일 찾는 중... 경로: {FILE_PATH}")
        return

    try:
        start_read = time.time()
        
        with open(FILE_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # 1. 리스트 변환 없이(Memory Safe) 끝까지 읽기
            last_row = None
            count = 0
            
            # [핵심] 여기서 파일을 처음부터 끝까지 한 줄씩 다 읽습니다.
            # 메모리는 안 터지지만, 파일이 크면 시간이 오래 걸립니다 (CPU/IO 부하).
            for row in reader:
                last_row = row
                count += 1
            
            if last_row is None: return

            # 2. 지연 시간 계산
            event_dt = datetime.fromisoformat(last_row['event_time'])
            now = datetime.now()
            lag = (now - event_dt).total_seconds()
            
            # 읽는데 걸린 시간 (순수 파일 읽기 시간)
            read_time = time.time() - start_read
            
            print(f"{count}건 스캔 | 지연(Lag): {lag:.2f}초 | 읽기소요: {read_time:.4f}초")

    except Exception as e:
        print(f"Error: {e}")

def main():
    print("🔎 [메모리 최적화 + CPU 고통] 분석기 시작...")
    while True:
        calculate_revenue()
        time.sleep(0.1) # 0.1초마다 재실행

if __name__ == "__main__":
    main()