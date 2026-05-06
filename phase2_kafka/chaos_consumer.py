import json
import os
import random
from datetime import datetime
from confluent_kafka import Consumer
import clickhouse_connect

ch_client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='admin')

conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'group.id': 'chaos-ingestion-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True # 🚨 이 자동 커밋이 얼마나 무서운 결과를 낳는지 보게 될 겁니다.
}
consumer = Consumer(conf)
consumer.subscribe(["ecommerce-cluster-logs"])

def main():
    print("🚀 [The Second Pain] 수동 컨슈머 가동. (언제 OOM Killer에 의해 죽을지 모릅니다)")
    batch_data = []
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            parsed = json.loads(msg.value().decode('utf-8'))
            dt_obj = datetime.fromisoformat(parsed['event_time'].replace('Z', '+00:00'))
            batch_data.append([dt_obj, parsed['user_id'], parsed['event_type'], parsed['price']])
            
            # 1만 개가 모이면 DB에 밀어 넣습니다.
            if len(batch_data) >= 10000:
                ch_client.insert('ecommerce.logs', batch_data, column_names=['event_time', 'user_id', 'event_type', 'price'])
                print("⚡ DB Insert 완료! (하지만 카프카 커밋은 아직 안 됨)")
                
                # -------------------------------------------------------------
                # 🚨 [대참사 시뮬레이션] 
                # DB에는 데이터가 들어갔지만, 카프카에 오프셋을 기록하기 전 프로세스 사망!
                # -------------------------------------------------------------
                if random.random() < 0.2: 
                    print("💥 [OOM 지옥] 메모리 한도 초과! 프로세스가 비정상 종료됩니다!")
                    os._exit(1)
                
                batch_data.clear()

    finally:
        consumer.close()

if __name__ == "__main__":
    main()