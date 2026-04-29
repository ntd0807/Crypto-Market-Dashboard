import json
import ssl
import urllib.parse
import urllib.request
import certifi
import pandas as pd
import os
from time import sleep

API_KEY = "YOUR_API_KEY"
FILE_PATH = "../data/crypto_historical_data.csv"

def api_runner():
    params = urllib.parse.urlencode({
        "start": "1",
        "limit": "15",
        "convert": "USD",
    })

    request = urllib.request.Request(
        f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?{params}",
        headers={
            "Accept": "application/json",
            "X-CMC_PRO_API_KEY": API_KEY,
        },
    )

    context = ssl.create_default_context(cafile=certifi.where())

    with urllib.request.urlopen(request, context=context) as response:
        data = json.load(response)

    df = pd.json_normalize(data["data"])
    df["timestamp"] = pd.Timestamp.now()

    df = df[
        [
            "id",
            "name",
            "symbol",
            "cmc_rank",
            "quote.USD.price",
            "quote.USD.volume_24h",
            "quote.USD.percent_change_1h",
            "quote.USD.percent_change_24h",
            "quote.USD.percent_change_7d",
            "quote.USD.market_cap",
            "timestamp",
        ]
    ]

    df = df.rename(columns={
        "quote.USD.price": "price_usd",
        "quote.USD.volume_24h": "volume_24h",
        "quote.USD.percent_change_1h": "pct_change_1h",
        "quote.USD.percent_change_24h": "pct_change_24h",
        "quote.USD.percent_change_7d": "pct_change_7d",
        "quote.USD.market_cap": "market_cap"
    })

    return df


def save_to_csv(df):
    write_header = not os.path.exists(FILE_PATH)

    df.to_csv(
        FILE_PATH,
        mode="a",
        header=write_header,
        index=False
    )


if __name__ == "__main__":
    for i in range(10):
        df = api_runner()
        save_to_csv(df)
        print("Data updated:", pd.Timestamp.now())
        sleep(60)