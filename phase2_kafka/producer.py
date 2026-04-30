import time
import json
import random
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer, KafkaError

fake = Faker()

conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': 'cluster-producer',
    'acks': 'all',
    'queue.buffering.max.messages': 1000000 # 👈 로컬 메모리 큐 한도 펌핑!
}
producer = Producer(conf)

def main():
    print("🚀 [폭주 모드 시작] 브레이크 박살! 초당 무제한 발사 시작!")
    topic_name = "ecommerce-cluster-logs"
    count = 0
    start_time = time.time()
    
    try:
        while True:
            log_data = {
                'event_time': datetime.now().isoformat(),
                'user_id': fake.uuid4(),
                'event_type': random.choice(['click', 'view', 'purchase']),
                'price': round(random.uniform(10.0, 500.0), 2)
            }
            
            try:
                # 콜백(callback)마저 빼버립니다. 극강의 속도를 위해!
                producer.produce(topic=topic_name, value=json.dumps(log_data).encode('utf-8'))
                producer.poll(0) 
            except BufferError:
                # 🚨 파이썬 메모리가 카프카 전송 속도를 못 따라갈 때 터지는 에러
                print("💥 [BufferError] 앗! 프로듀서 큐가 꽉 찼습니다. 강제 1초 휴식!")
                producer.poll(1)
                continue

            count += 1
            if count % 10000 == 0:
                elapsed = time.time() - start_time
                tps = 10000 / elapsed
                print(f"🔥 10,000건 발사 완료! (소요 시간: {elapsed:.2f}초, TPS: {tps:.0f}건/초)")
                start_time = time.time()

    except KeyboardInterrupt:
        print("\n🛑 시스템 종료 중... 남은 데이터 털어내는 중 (오래 걸릴 수 있음)...")
        producer.flush()

if __name__ == "__main__":
    main()