from fast_trade import run_backtest
import pprint

strategy = {
    "freq": "1h",
    "any_enter": [],
    "any_exit": [],
    "enter": [
        "rsi < 30",
        "bbands_bbands_bb_lower > close",
    ],
    "exit": [
        "rsi > 70",
        "bbands_bbands_bb_upper < close",
    ],
    "datapoints": [
        {"name": "ema", "transformer": "ema", "args": [5]},
        {"name": "sma", "transformer": "sma", "args": [20]},
        {"name": "rsi", "transformer": "rsi", "args": [14]},
        {"name": "obv", "transformer": "obv", "args": []},
        {"name": "bbands", "transformer": "bbands", "args": [20, 2]},
    ],
    "base_balance": 1000.0,
    "exit_on_end": False,
    "comission": 0.0,
    "trailing_stop_loss": 0.0,
    "lot_size_perc": 1.0,
    "max_lot_size": 0.0,
    "start_date": "2025-06-01",
    "end_date": "2025-09-20",
    "rules": None,
    "symbol": "BTCUSDT",
    "exchange": "binance",
}

if __name__ == "__main__":
    res = run_backtest(strategy)
    pprint.pprint(res.get("summary"))
