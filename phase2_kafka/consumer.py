import json
from confluent_kafka import Consumer, KafkaError
import clickhouse_connect 
from datetime import datetime

# 1. 클릭하우스 연결 설정
print("🔌 클릭하우스에 연결 중...")
# 포트 8123은 아까 docker-compose에서 뚫어놓은 클릭하우스 HTTP 통신 포트입니다.
ch_client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='admin')
print("✅ 클릭하우스 연결 성공!")

# 2. 카프카 소비자 설정
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'group.id': 'clickhouse-ingestion-team',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}
consumer = Consumer(conf)
topic_name = "ecommerce-cluster-logs"
consumer.subscribe([topic_name])

def main():
    print(f"🚀 카프카 데이터를 클릭하우스로 밀어 넣습니다! (Ctrl+C로 종료)")
    
    batch_data = [] # 📦 데이터를 모아둘 바구니
    BATCH_SIZE = 20 # ⚡ 20개가 모이면 한 번에 쏜다! (클릭하우스는 한 건씩 넣으면 싫어함)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                # 1초 기다렸는데 새 데이터가 없고, 바구니에 남은 데이터가 있다면 마저 털어넣기
                if len(batch_data) > 0:
                    ch_client.insert('ecommerce.logs', batch_data, column_names=['event_time', 'user_id', 'event_type', 'price'])
                    print(f"🧹 [남은 찌꺼기 털기] {len(batch_data)}개 데이터 저장 완료!")
                    batch_data.clear()
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ 에러 발생: {msg.error()}")
                    break

            # 3. JSON 파싱
            raw_data = msg.value().decode('utf-8')
            parsed_data = json.loads(raw_data)
            
            time_str = parsed_data['event_time'].replace('Z', '+00:00')
            dt_obj = datetime.fromisoformat(time_str)
            # 4. 클릭하우스 테이블 순서에 맞게 리스트로 포장
            row = [
                dt_obj,
                parsed_data['user_id'],
                parsed_data['event_type'],
                parsed_data['price']
            ]
            
            batch_data.append(row) # 바구니에 담기
            
            # 5. 바구니가 꽉 차면(20개) 클릭하우스로 발사! (Batch Insert)
            if len(batch_data) >= BATCH_SIZE:
                ch_client.insert('ecommerce.logs', batch_data, column_names=['event_time', 'user_id', 'event_type', 'price'])
                print(f"⚡ [Batch Insert] {BATCH_SIZE}개 데이터 클릭하우스 저장 완료! (최근 Offset: {msg.offset()})")
                batch_data.clear() # 다 쐈으면 바구니 비우기

    except KeyboardInterrupt:
        print("\n🛑 시스템 종료 중... 바구니에 남은 데이터 마저 털어넣기...")
        if len(batch_data) > 0:
            ch_client.insert('ecommerce.logs', batch_data, column_names=['event_time', 'user_id', 'event_type', 'price'])
            print(f"📦 마지막 {len(batch_data)}개 데이터 저장 완료!")
    
    finally:
        consumer.close()

if __name__ == "__main__":
    main()