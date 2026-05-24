from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
from app.utils.logger import get_logger

logger = get_logger("producer")


class AudienceEventProducer:

    def __init__(self, broker="localhost:9092", topic="audience-events"):
        self.topic = topic

        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            linger_ms=10
        )

        logger.info(f"Kafka Producer initialized for topic: {self.topic}")

    def generate_event(self):

        return {
            "user_id": random.randint(1, 1000),
            "country": random.choice(["Germany", "France", "UK", "USA"]),
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "session_duration": random.randint(5, 300),
            "timestamp": datetime.utcnow().isoformat()
        }

    def send_event(self, event):

        try:
            future = self.producer.send(self.topic, value=event)
            self.producer.flush()

            record_metadata = future.get(timeout=10)

            logger.info(
                f"Event sent | topic={record_metadata.topic} "
                f"partition={record_metadata.partition} "
                f"offset={record_metadata.offset}"
            )

        except Exception as e:
            logger.error(f"Failed to send event: {e}")

    def run(self, interval=2):

        logger.info("Producer started...")

        while True:
            event = self.generate_event()
            self.send_event(event)
            time.sleep(interval)


if __name__ == "__main__":
    producer = AudienceEventProducer()
    producer.run()