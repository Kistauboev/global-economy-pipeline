import json
import pandas as pd
from pathlib import Path
from kafka import KafkaConsumer

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "raw_data.csv"

TOPIC = "global-economy"
BOOTSTRAP_SERVERS = "localhost:9092"


def consume_data(expected_count=None):
    if expected_count is None:
        expected_count = len(pd.read_csv(DATA_PATH))

    consumer = None
    records = []

    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=None,
            consumer_timeout_ms=15000,
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            )
        )

        print("Consumer connected to Kafka")

        for message in consumer:
            records.append(message.value)

            if len(records) >= expected_count:
                break

        print(f"Consumed messages: {len(records)}")

        if len(records) != expected_count:
            raise RuntimeError(
                f"Expected {expected_count} messages, "
                f"received {len(records)}"
            )

        df = pd.DataFrame(records)

        print("\nReceived data sample:")
        print(df.head())

        return df

    except Exception as e:
        print(f"Consumer error: {e}")
        raise

    finally:
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":
    consume_data()
