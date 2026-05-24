from kafka import KafkaConsumer
import json
import pandas as pd
import os

os.makedirs("data/bronze", exist_ok=True)

consumer = KafkaConsumer(
    'audience-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:

    data = message.value

    df = pd.DataFrame([data])

    file_exists = os.path.isfile("data/bronze/events.csv")

    df.to_csv(
        "data/bronze/events.csv",
        mode='a',
        header=not file_exists,
        index=False
    )

    print(f"Consumed: {data}")