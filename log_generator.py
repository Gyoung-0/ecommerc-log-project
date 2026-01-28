import time
import csv
import random
import os
from datetime import datetime
from faker import Faker

# 1. initialize Faker generator
fake = Faker()

# 2. File path to save logs
FILE_PATH = "user_log.csv"

def generate_log():
    """
    Generates a single log enrty as a dictionary
    """
    event_types = ['click', 'view', 'purchase']
    current_event = random.choice(event_types)
    
    # Generate price and product_id only for 'purchase' events.
    # Otherwise, set them to None or -
    if current_event == 'purchase':
        product_id = fake.ean8() # 8-digit product code
        price = round(random.uniform(10.0 , 500.0), 2) # Price between $10 and $500
    else:
        product_id = None
        price = 0
        
    return {
        'event_time': datetime.now().isoformat(), # Current timestamp
        'user_id': fake.uuid4(),                  # Random User ID
        'event_type': current_event,
        'product_id': product_id,
        'price': price
    }
    
def main():
    print(f"Starting Log Generator... (Target File: {FILE_PATH})")
    
    # If the file doesn`t exist, create it and write the CSV header first
    if not os.path.exists(FILE_PATH):
        with open(FILE_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['event_time', 'user_id', 'event_type', 'product_id', 'price'])
            writer.writeheader()
            
    # 3. Infinite loop: Runs until the program is manually stopped
    try:
        while True:
            log_data = generate_log()
            
            # Open file in 'a' (append) mode to add data
            # NOTE: Opening and closing the file for evenry single line is inefficient
            # We will experience the pain of tihs approach later
            with open(FILE_PATH, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=log_data.keys())
                writer.writerow(log_data)
            print(f"Log generated: {log_data['event_type']}")
            
            # Sleep for 0.1 seconds (Generates approx. 10 logs per second)
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\n Log generation stopped")
        
if __name__ == "__main__":
    main()