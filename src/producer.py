from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(100):
    message = {'number': i}
    producer.send('test-topic', value=message)
    print(f"Produced: {message}")
    time.sleep(1)

producer.flush()
