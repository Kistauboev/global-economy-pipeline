import json
import argparse
import pandas as pd
from kafka import KafkaConsumer

TOPIC = "global-economy"
BOOTSTRAP_SERVERS = "localhost:9092"

def consume_data(expected_count, run_id):

    consumer = None
    records = []

    if expected_count <= 0:
        raise ValueError("Expected message count must be positive")

    try:

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=None,
            consumer_timeout_ms=30000,
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            )
        )

        print("Consumer connected to Kafka")
        print(f"Run ID: {run_id}")

        for message in consumer:

            headers = dict(message.headers)

            message_run_id = headers.get("run_id")

            if message_run_id is None:
                continue

            if message_run_id.decode("utf-8") != run_id:
                continue

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

    except Exception as error:
        print(f"Consumer error: {error}")
        raise

    finally:
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("expected_count", type=int)
    parser.add_argument("run_id")

    args = parser.parse_args()

    consume_data(
        args.expected_count,
        args.run_id
    )
