import json
import time

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
for i in range(10):
    producer.send('test', str(i))
    producer.flush()
    time.sleep(2)
    producer.send('test', str(i))
    producer.flush()

