from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

countries = ['Germany', 'France', 'UK', 'USA']
devices = ['mobile', 'desktop', 'tablet']

while True:

    event = {
        'user_id': random.randint(1, 1000),
        'country': random.choice(countries),
        'device': random.choice(devices),
        'session_duration': random.randint(5, 300),
        'timestamp': datetime.utcnow().isoformat()
    }

    producer.send('audience-events', value=event)

    print(f"Produced: {event}")

    time.sleep(2)