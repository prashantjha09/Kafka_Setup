from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers=[
        "kafka-0.kafka.kafka.svc.cluster.local:9092",
        "kafka-1.kafka.kafka.svc.cluster.local:9092",
        "kafka-2.kafka.kafka.svc.cluster.local:9092",
    ]
)

topic = "test-topic"

print("Starting producer...")
count = 0

while True:
    msg = f"message number {count}".encode("utf-8")
    producer.send(topic, msg)
    producer.flush()
    print(f"Sent: {msg}")
    count += 1
    time.sleep(2)  # wait 2 seconds
