
import requests
import pandas as pd
from pathlib import Path

BASE_URL = "https://api.worldbank.org/v2"

INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP",
    "NY.GDP.PCAP.CD": "GDP per capita",
    "FP.CPI.TOTL.ZG": "Inflation",
    "SL.UEM.TOTL.ZS": "Unemployment",
    "SP.POP.TOTL": "Population"
}

START_YEAR = 2015
END_YEAR = 2024


def fetch_world_bank_data():

    indicator_codes = ";".join(INDICATORS.keys())

    url = f"{BASE_URL}/country/all/indicator/{indicator_codes}"

    params = {
        "format": "json",
        "source": 2,
        "date": f"{START_YEAR}:{END_YEAR}",
        "per_page": 20000
    }

    all_records = []
    page = 1

    while True:

        params["page"] = page

        try:
            response = requests.get(
                url,
                params=params,
                timeout=60
            )

            response.raise_for_status()
            result = response.json()

            if not isinstance(result, list) or len(result) < 2:
                raise ValueError("Unexpected API response")

            metadata, observations = result

            if observations:
                for item in observations:

                    if item["country"].get("region", {}).get("value") == "Aggregates":
                        continue

                    all_records.append({
                        "country": item["country"]["value"],
                        "country_code": item["countryiso3code"],
                        "year": int(item["date"]),
                        "indicator_code": item["indicator"]["id"],
                        "indicator": INDICATORS[item["indicator"]["id"]],
                        "value": item["value"]
                    })

            if page >= metadata["pages"]:
                break

            page += 1

        except requests.RequestException as error:
            raise RuntimeError(
                f"API request failed: {error}"
            ) from error

    return pd.DataFrame(all_records)


def save_raw_data(df):

    project_root = Path(__file__).resolve().parent.parent
    output_path = project_root / "data" / "raw_data.csv"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)

    print(f"\nData saved to: {output_path}")
    print(f"Total records: {len(df)}")


if __name__ == "__main__":

    df = fetch_world_bank_data()

    print("\nFirst 5 records:")
    print(df.head())

    print("\nDataset information:")
    print(df.info())

    save_raw_data(df)
