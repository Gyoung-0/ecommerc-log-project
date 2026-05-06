import time
import json
import random
from datetime import datetime
import uuid
from confluent_kafka import Producer

# 메모리 큐 버퍼를 극한으로 늘려 OOM을 지연시키고 카프카에 때려 박습니다.
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': 'chaos-producer',
    'acks': 'all',
    'queue.buffering.max.messages': 500000 
}
producer = Producer(conf)

def main():
    print("🚀 [The First Pain] 인메모리 큐였다면 진작 터졌을 트래픽을 쏟아붓습니다.")
    topic_name = "ecommerce-cluster-logs"
    count = 0
    start_time = time.time()
    
    try:
        while count < 1000000:
            log_data = {
                'event_time': datetime.now().isoformat(),
                'user_id': str(uuid.uuid4()),
                'event_type': random.choice(['click', 'view', 'purchase', 'add_to_cart']),
                'price': round(random.uniform(5.0, 999.0), 2)
            }
            
            try:
                producer.produce(topic=topic_name, value=json.dumps(log_data).encode('utf-8'))
                producer.poll(0)
            except BufferError:
                producer.poll(0.5) # 버퍼 풀 시 역압 조절 흉내
                continue

            count += 1
            if count % 100000 == 0:
                print(f"🔥 {count}건 전송 완료... (Kafka Broker가 버티는지 확인하세요)")

        print(f"🎯 1,000,000건 발사 완료. (소요 시간: {time.time() - start_time:.2f}초)")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()