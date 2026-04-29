import json
from confluent_kafka import Consumer, KafkaError

# 1. 소비자(Consumer) 설정: "나는 누구고, 어디서, 어떻게 읽을 것인가?"
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094', # 클러스터 주소 3개
    'group.id': 'analytics-team-1',         # [핵심 1] 컨슈머 그룹 이름 (알바생 조 이름)
    'auto.offset.reset': 'earliest',        # [핵심 2] 처음 왔으면 맨 앞(0번)부터 다 내놔!
    'enable.auto.commit': True              # 다 읽었으면 알아서 책갈피(Offset) 꽂아둬!
}

# 소비자 고용
consumer = Consumer(conf)

def main():
    topic_name = "ecommerce-cluster-logs"
    
    # 2. 창고(Topic) 구독 신청 (이제부터 이 창고만 파보겠다!)
    consumer.subscribe([topic_name])
    
    print(f"🕵️‍♂️ '{conf['group.id']}' 그룹이 '{topic_name}' 창고를 털기 시작합니다! (Ctrl+C로 종료)")

    try:
        while True:
            # 3. 창고에서 물건 빼오기 (1초 동안 기다려보고 없으면 빈손으로 돌아옴)
            msg = consumer.poll(1.0)

            if msg is None:
                continue # 1초 기다렸는데 물건 없으면 다시 루프 돌기
            
            if msg.error():
                # 에러 중에서도 '파티션 끝에 도달함(EOF)' 에러는 무시
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ 에러 발생: {msg.error()}")
                    break

            # 4. 물건(데이터)이 정상적으로 도착했을 때!
            # 카프카는 데이터를 바이트(Byte)로 주니까 다시 문자열(utf-8)로 풀고 JSON으로 변환
            raw_data = msg.value().decode('utf-8')
            parsed_data = json.loads(raw_data)

            print(f"📥 [수신 완료] Offset: {msg.offset()} | Event: {parsed_data['event_type']} | Price: ${parsed_data['price']}")

    except KeyboardInterrupt:
        print("\n🛑 분석기 종료 중... 마지막으로 읽은 책갈피(Offset)를 저장합니다...")
    
    finally:
        # 5. [매우 중요] 퇴근할 때 문 단속 잘하고 책갈피 확실히 꽂아두기
        consumer.close()

if __name__ == "__main__":
    main()