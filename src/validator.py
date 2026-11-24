
import pandas as pd
import pandera.pandas as pa
import numpy as np
from pathlib import Path


INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP",
    "NY.GDP.PCAP.CD": "GDP per capita",
    "FP.CPI.TOTL.ZG": "Inflation",
    "SL.UEM.TOTL.ZS": "Unemployment",
    "SP.POP.TOTL": "Population"
}

VALID_INDICATOR_CODES = list(INDICATORS.keys())
VALID_INDICATOR_NAMES = list(INDICATORS.values())


schema = pa.DataFrameSchema(
    {
        "country": pa.Column(
            str,
            checks=pa.Check(
                lambda s: s.str.strip().ne("").all()
            )
        ),

        "country_code": pa.Column(
            str,
            checks=pa.Check(
                lambda s: s.dropna().str.fullmatch(r"[A-Z]{3}").all()
            ),
            nullable=True
        ),

        "year": pa.Column(
            int,
            checks=pa.Check.in_range(2015, 2024),
            coerce=True
        ),

        "indicator_code": pa.Column(
            str,
            checks=pa.Check.isin(VALID_INDICATOR_CODES)
        ),

        "indicator": pa.Column(
            str,
            checks=pa.Check.isin(VALID_INDICATOR_NAMES)
        ),

        "value": pa.Column(
            float,
            checks=pa.Check(
                lambda s: np.isfinite(s.dropna()).all()
            ),
            nullable=True,
            coerce=True
        )
    },

    checks=pa.Check(
        lambda df: (
            df["indicator_code"].map(INDICATORS) == df["indicator"]
        ).all()
    ),

    strict=True
)


def validate_data(df):
    return schema.validate(df, lazy=True)


if __name__ == "__main__":

    project_root = Path(__file__).resolve().parent.parent
    data_path = project_root / "data" / "raw_data.csv"

    df = pd.read_csv(data_path)

    print("Validating real dataset...")

    try:
        validated_df = validate_data(df)

        print("SUCCESS: Dataset passed validation!")
        print("Total records:", len(validated_df))

    except pa.errors.SchemaErrors as error:
        print("VALIDATION FAILED!")
        print(error.failure_cases)
        raise

    # Three deliberately invalid test records
    invalid_records = pd.DataFrame([
        {
            "country": "Test Country",
            "country_code": "USA",
            "year": 2025,
            "indicator_code": "NY.GDP.MKTP.CD",
            "indicator": "GDP",
            "value": 1000
        },
        {
            "country": "Test Country",
            "country_code": "INVALID",
            "year": 2020,
            "indicator_code": "NY.GDP.MKTP.CD",
            "indicator": "GDP",
            "value": 1000
        },
        {
            "country": "Test Country",
            "country_code": "USA",
            "year": 2020,
            "indicator_code": "INVALID",
            "indicator": "Unknown",
            "value": 1000
        }
    ])

    print("\nTesting 3 deliberately invalid records...")

    try:
        validate_data(invalid_records)
        print("WARNING: Invalid records passed unexpectedly!")

    except pa.errors.SchemaErrors as error:
        print("SUCCESS: Pandera detected invalid records!")
        print("\nFailure details:")
        print(error.failure_cases)
