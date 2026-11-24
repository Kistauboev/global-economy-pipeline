import json
import pandas as pd
from pathlib import Path
from kafka import KafkaProducer

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "raw_data.csv"

TOPIC = "global-economy"
BOOTSTRAP_SERVERS = "localhost:9092"


def produce_data(records=None):
    if records is None:
        df = pd.read_csv(DATA_PATH)
        records = json.loads(df.to_json(orient="records"))

    producer = None
    sent_count = 0

    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(
                value, allow_nan=False
            ).encode("utf-8"),
            acks="all",
            retries=3
        )

        print("Connected to Kafka successfully")

        futures = []

        for record in records:
            future = producer.send(TOPIC, value=record)
            futures.append(future)

        producer.flush(timeout=60)

        for future in futures:
            future.get(timeout=30)
            sent_count += 1

        print(f"Published messages: {sent_count}")

        return sent_count

    except Exception as e:
        print(f"Producer error: {e}")
        print(f"Confirmed messages: {sent_count}")
        raise

    finally:
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    produce_data()
