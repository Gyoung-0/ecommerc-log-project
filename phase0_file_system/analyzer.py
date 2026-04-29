import time
import os

FILE_PATH = "data/user_log.csv"

def main():
    print("🔎 [실시간 스트리밍] 분석기 시작... (파일 핸들 유지)", flush=True)
    
    # 파일 생길 때까지 대기
    while not os.path.exists(FILE_PATH):
        time.sleep(1)

    # 🔥 [핵심] 파일을 딱 한 번만 열고 절대 닫지 않음 (tail -f 방식)
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        # 헤더 건너뛰기
        f.readline()
        
        while True:
            line = f.readline()
            if line:
                print(f"✅ 읽음: {line.strip()[:50]}...", flush=True)
            else:
                # 데이터가 없으면 잠깐 대기 (Polling)
                time.sleep(0.1)

if __name__ == "__main__":
    main()