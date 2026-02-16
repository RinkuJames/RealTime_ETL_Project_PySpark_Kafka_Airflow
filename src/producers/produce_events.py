from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'realtime-events'

# Sample data generator
while True:
    data = {
        "customer_id": random.randint(1000, 2000),
        "event_type": random.choice(["login", "purchase", "logout"]),
        "amount": round(random.uniform(10, 500), 2),
        "timestamp": int(time.time())
    }
    producer.send(topic, value=data)
    print(f"Sent: {data}")
    time.sleep(2)  # every 2 seconds
