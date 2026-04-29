import time
import json
import random
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# 1. 카프카 클러스터 연결 설정 (1, 2, 3호기 주소 모두 입력)
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': 'cluster-producer',
    'acks': 'all'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ 전송 실패: {err}")
    else:
        # 이번엔 파티션과 오프셋(번호표)까지 출력해 봅시다!
        print(f"✅ [전송 완료] Topic: {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}")

def main():
    print("🚀 카프카 클러스터(3-Broker)로 로그 전송 시작! (Ctrl+C로 종료)")
    topic_name = "ecommerce-cluster-logs" # 우리가 방금 만든 3중 복제 창고 이름!
    
    try:
        while True:
            log_data = {
                'event_time': datetime.now().isoformat(),
                'user_id': fake.uuid4(),
                'event_type': random.choice(['click', 'view', 'purchase']),
                'price': round(random.uniform(10.0, 500.0), 2)
            }
            
            json_data = json.dumps(log_data)
            
            producer.produce(topic=topic_name, value=json_data.encode('utf-8'), callback=delivery_report)
            producer.poll(0)
            
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n🛑 시스템 종료 중... 남은 데이터 밀어내기...")
        producer.flush()

if __name__ == "__main__":
    main()