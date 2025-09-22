import datetime
import math
import os
import random
import time
from typing import List, Optional
from datetime import UTC

import pandas as pd
import requests

API_DELAY = float(os.getenv("API_DELAY", 0.3))

# Cache for available symbols with timestamp
_available_symbols_cache: Optional[List[str]] = None
_cache_timestamp: Optional[datetime.datetime] = None
CACHE_DURATION = datetime.timedelta(hours=1)  # Cache for 1 hour

BINANCE_KLINE_REST_HEADER_MATCH = [
    "date",  # Open time
    "open",  # Open
    "high",  # High
    "low",  # Low
    "close",  # Close
    "volume",  # Volume
    "close_time",  # Close time
    "quote_asset_volume",  # Quote asset volume
    "number_of_trades",  # Number of trades
    "taker_buy_base_asset_volume",  # Taker buy base asset volume
    "taker_buy_base_a_volume",  # Taker buy quote
    "ignore",  # literally ignore this
]


def get_exchange_info():
    url = "https://api.binance.com/api/v3"
    req = requests.get(f"{url}/exchangeInfo")
    # attempt to sort any keys that are lists
    data = req.json()

    def sort_data(data):
        if isinstance(data, dict):
            return {k: sort_data(v) for k, v in sorted(data.items())}
        elif isinstance(data, list):
            new_list = []
            for x in data:
                if isinstance(x, (dict, list)):
                    new_list.append(sort_data(x))
                else:
                    new_list.append(x)
            return (
                sorted(new_list, key=str)
                if all(isinstance(i, dict) for i in new_list)
                else sorted(new_list)
            )
        else:
            return data

    # print(data)
    data = sort_data(data)

    return data


def get_available_symbols() -> List[str]:
    global _available_symbols_cache, _cache_timestamp

    now = datetime.datetime.now()

    # Check if cache is valid
    if (
        _available_symbols_cache is not None
        and _cache_timestamp is not None
        and now - _cache_timestamp < CACHE_DURATION
    ):
        return _available_symbols_cache

    # Cache miss or expired, fetch from API
    exchange_info = get_exchange_info()
    symbols = []

    for symbol in exchange_info.get("symbols", []):
        if symbol["status"] == "TRADING":
            symbols.append(symbol["symbol"])

    symbols.sort()

    # Update cache
    _available_symbols_cache = symbols
    _cache_timestamp = now

    return symbols


def get_oldest_date_available(symbol):
    endTime = int(datetime.datetime.now(UTC).timestamp() * 1000)
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&startTime=0&endTime={endTime}&limit=1"

    data = requests.get(url).json()
    try:
        oldest_date = datetime.datetime.fromtimestamp(data[0][0] / 1000)
        return oldest_date
    except Exception:
        print(f"error with {symbol}")
        return datetime.datetime.now(UTC) - datetime.timedelta(days=1)


def get_binance_klines(
    symbol,
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    status_update=lambda x: None,
    store_func=lambda x, y: None,
):
    start_date = start_date.replace(tzinfo=datetime.timezone.utc)
    end_date = end_date.replace(tzinfo=datetime.timezone.utc)

    curr_date = start_date

    HOURS_TO_INCREMENT = 15

    end_date = end_date.replace(tzinfo=datetime.timezone.utc)
    now = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc)
    if end_date > now:
        end_date = now.replace(second=0, microsecond=0)

    # calculate the estimated number of calls
    total_duration_hours = (end_date - curr_date).total_seconds() / 3600
    num_calls = math.ceil(total_duration_hours / HOURS_TO_INCREMENT)
    total_api_calls = 0
    klines = []
    start_time = time.time()
    error_count = 0
    while curr_date < end_date:
        next_end_date = curr_date + datetime.timedelta(hours=HOURS_TO_INCREMENT)
        startTime = int(curr_date.timestamp()) * 1000
        endTime = int(next_end_date.timestamp()) * 1000

        print(f"Getting {symbol} from {curr_date} to {next_end_date}")

        url = (
            "https://api.binance.com/api/v3/klines"
            f"?symbol={symbol}&interval=1m"
            f"&startTime={startTime}&endTime={endTime}&limit=1000"
        )

        req = requests.get(url)
        total_api_calls += 1
        if req.status_code == 200:
            curr_date = next_end_date
            klines.extend(req.json())
            error_count = 0  # Reset error count on successful request
        else:
            # print(req)
            # raise Exception(f"Error with {symbol}")
            print(f"Error: {symbol} {req.text}")
            error_count += 1
            if error_count > 3:
                raise Exception(
                    f"Download failed for {symbol} after 3 errors. Error: {req.text}"
                )
            if req.status_code == 429:
                # sleeep for some time
                time.sleep(10)
                continue
        sleeper = random.random() * API_DELAY
        if sleeper < 0.1:
            sleeper += 0.1

        if total_api_calls % 30 == 0:
            sleeper += random.randint(1, 3)
            kline_df = binance_kline_to_df(klines)
            store_func(kline_df, symbol, "binance")

        status_obj = {
            "symbol": symbol,
            "perc_complete": round(total_api_calls / num_calls * 100, 2),
            "call_count": total_api_calls,
            "total_calls": num_calls,
            "total_time": round(time.time() - start_time, 2),
            # "sleep_time": sleeper,
            "est_time_remaining": round(
                (time.time() - start_time)
                / total_api_calls
                * (num_calls - total_api_calls),
                2,
            ),
        }
        status_update(status_obj)
        time.sleep(sleeper)

    status_obj = {
        "symbol": symbol,
        "perc_complete": 100,
        "call_count": total_api_calls,
        "total_calls": total_api_calls,
        "total_time": time.time() - start_time,
        "est_time_remaining": 0,
    }
    status_update(status_obj)
    klines_df = binance_kline_to_df(klines)
    return klines_df, status_obj


def binance_kline_to_df(klines):
    new_df = pd.DataFrame(klines, columns=BINANCE_KLINE_REST_HEADER_MATCH)

    new_df = new_df.drop_duplicates()
    new_df.index = pd.to_datetime(new_df.date, unit="ms")

    columns_to_drop = []
    if new_df.ignore.any():
        columns_to_drop.append("ignore")

    if new_df.date.any():
        columns_to_drop.append("date")

    new_df = new_df.drop(columns=columns_to_drop)

    return new_df
