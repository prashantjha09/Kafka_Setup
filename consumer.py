from kafka import KafkaConsumer
import time

KAFKA_BOOTSTRAP_SERVERS = [
    "kafka-0.kafka.kafka.svc.cluster.local:9092",
    "kafka-1.kafka.kafka.svc.cluster.local:9092",
    "kafka-2.kafka.kafka.svc.cluster.local:9092",
]

TOPIC_NAME = "test-topic"
CONSUMER_GROUP = "test-consumer-group"   # Proper consumer group
CLIENT_ID = "consumer-inside-k8s-1"      # Unique identifier for this consumer


def start_consumer():
    while True:
        try:
            print("Connecting to Kafka...")

            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=CONSUMER_GROUP,
                client_id=CLIENT_ID,
                auto_offset_reset="earliest",   # start from beginning if no offset stored
                enable_auto_commit=True,         # commit offsets automatically
                max_poll_records=10,             # batch size
                consumer_timeout_ms=10000        # timeout to break internal loop
            )

            print("✅ Kafka consumer connected and started...")
            print(f"Listening to topic: {TOPIC_NAME}\n")

            for message in consumer:
                try:
                    decoded_value = message.value.decode("utf-8")
                    print(
                        f"[{message.timestamp}] "
                        f"Partition:{message.partition} "
                        f"Offset:{message.offset} "
                        f"Value:{decoded_value}"
                    )
                except Exception as e:
                    print(f"❌ Failed to decode message: {e}")

        except Exception as e:
            # Handles connection failures, broker unavailable etc.
            print(f"❌ Kafka connection error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
        finally:
            try:
                consumer.close()
            except:
                pass


if __name__ == "__main__":
    start_consumer()
