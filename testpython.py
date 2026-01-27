import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
from io import StringIO
from dotenv import load_dotenv
import os


load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

INSTRUMENT_URL = "https://api.dhan.co/v2/instrument/NSE_FNO"
HIST_URL = "https://api.dhan.co/v2/charts/intraday"

HEADERS = {
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN
}


def fetch_fut_candle():
    payload = {
        "securityId": "49229",
        "exchangeSegment": "NSE_FNO",
        "instrument": "FUTIDX",
        "interval": "1",
        "oi": True,
        "fromDate": "2026-01-01 09:14:00",
        "toDate": "2026-01-01 09:16:00"
    }

    r = requests.post(HIST_URL, headers=HEADERS, json=payload)
    data = r.json()

    if not data.get("timestamp"):
        return None

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"]
    })

    dt_index = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt_index.dt.tz_convert("Asia/Kolkata")
    row = df.iloc[0].copy()
    print(row)
    return row


fetch_fut_candle()