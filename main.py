import json

from src.api_client import fetch_world_bank_data, save_raw_data
from src.producer import produce_data
from src.consumer import consume_data
from src.validator import validate_data


def main():
    print("GLOBAL ECONOMY ANALYSIS")
    print("=" * 50)

    print("\n1. Collecting World Bank data")
    raw_df = fetch_world_bank_data()
    save_raw_data(raw_df)

    print(f"Collected records: {len(raw_df)}")

    print("\n2. Publishing messages to Kafka")
    records = json.loads(
        raw_df.to_json(orient="records")
    )
    sent_count = produce_data(records)

    print("\n3. Consuming messages from Kafka")
    received_df = consume_data(sent_count)

    print("\n4. Validating data with Pandera")
    valid_df = validate_data(received_df)

    print("\nPIPELINE RESULTS")
    print("=" * 50)
    print(f"Collected records: {len(raw_df)}")
    print(f"Published messages: {sent_count}")
    print(f"Consumed messages: {len(received_df)}")
    print(f"Validated records: {len(valid_df)}")

    print("\nKafka and validation stages completed")

    return valid_df


if __name__ == "__main__":
    main()
