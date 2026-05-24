from kafka import KafkaConsumer
import json
import pandas as pd
import os
import time
from app.utils.logger import get_logger

# Logger
logger = get_logger("kafka_consumer")

# Ensure directory exists
BRONZE_PATH = "data/bronze"
os.makedirs(BRONZE_PATH, exist_ok=True)

FILE_PATH = os.path.join(BRONZE_PATH, "events.csv")

# Kafka Consumer
consumer = KafkaConsumer(
    'audience-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id="audience-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

logger.info("Kafka Consumer started... listening to topic")

batch = []
BATCH_SIZE = 10

try:
    for message in consumer:

        data = message.value
        batch.append(data)

        logger.info(f"Consumed event: {data}")

        # Write in batches (better performance)
        if len(batch) >= BATCH_SIZE:

            df = pd.DataFrame(batch)

            file_exists = os.path.isfile(FILE_PATH)

            df.to_csv(
                FILE_PATH,
                mode='a',
                header=not file_exists,
                index=False
            )

            logger.info(f"Wrote batch of {len(batch)} records to Bronze layer")

            batch.clear()

        time.sleep(0.1)

except Exception as e:
    logger.error(f"Consumer failed: {str(e)}")

finally:
    consumer.close()
    logger.info("Kafka consumer closed safely")