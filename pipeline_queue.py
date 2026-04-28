import time
import random
from multiprocessing import Process, Queue
from datetime import datetime
from faker import Faker

fake = Faker()

def generator_task(queue):
    print("======생산자 프로세스 가동========")
    while True:
        log_data = {
            'event_time': datetime.now().isoformat(),
            'user_id': fake.uuid4(),
            'event_type': random.choice(['click', 'view', 'purchase']),
            'price': round(random.uniform(10.0, 500.0), 2)
        }
        
        queue.put(log_data)
        print(f"{log_data} was threw in queue")
        
        time.sleep(0.1)
        
def analyzer_task(queue):
    print("consumer process")
    while True:
        data = queue.get()
        
        print("출력 완료")
        
if __name__ == '__main__':
    print("인메모리 파이프라인 시스템 시작")
    2
    pipeline_queue = Queue()
    
    p1 = Process(target=generator_task, args=(pipeline_queue,))
    p2 = Process(target=analyzer_task, args=(pipeline_queue,))
    
    p1.start()
    p2.start()
    
    try:
        p1.join()
        p2.join()
        
    except KeyboardInterrupt:
        p1.terminate()
        p2.terminate
        print("프로세스 종료")