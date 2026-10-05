import argparse
import os

import duckdb


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--path",
        required=True,
        help="S3 path to raw CSV file",
    )

    args = parser.parse_args()

    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]
    s3_endpoint = os.environ["S3_ENDPOINT"]

    con = duckdb.connect()

    # Расширение для работы с S3 / HTTP
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    con.execute(
        f"SET s3_access_key_id='{s3_access_key}';"
    )

    con.execute(
        f"SET s3_secret_access_key='{s3_secret_key}';"
    )

    con.execute(
        f"SET s3_endpoint='{s3_endpoint}';"
    )

    con.execute(
        "SET s3_use_ssl=false;"
    )

    con.execute(
        "SET s3_url_style='path';"
    )

    query = f"""
        SELECT *
        FROM read_csv(
            '{args.path}',
            header=false,
            skip=1,
            names=[
                'Date',
                'Rented Bike Count',
                'Hour',
                'Temperature(°C)',
                'Humidity(%)',
                'Wind speed (m/s)',
                'Visibility (10m)',
                'Dew point temperature(°C)',
                'Solar Radiation (MJ/m2)',
                'Rainfall(mm)',
                'Snowfall (cm)',
                'Seasons',
                'Holiday',
                'Functioning Day'
            ],
            auto_detect=true
        )
        LIMIT 10
    """

    result = con.execute(query)

    columns = [
        column[0]
        for column in result.description
    ]

    print("Columns:")
    print(columns)

    print("\nFirst 10 rows:")

    for row in result.fetchall():
        print(row)

    con.close()


if __name__ == "__main__":
    main()
