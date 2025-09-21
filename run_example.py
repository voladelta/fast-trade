from fast_trade import run_backtest
from fast_trade.utils import parse_logic_expr
import pprint
import datetime

strategy = {
    "freq": "5Min",
    "any_enter": [],
    "any_exit": [],
    "enter": [
        parse_logic_expr("rsi < 30"),
        parse_logic_expr("bbands_bbands_bb_lower > close"),
    ],
    "exit": [
        parse_logic_expr("rsi > 70"),
        parse_logic_expr("bbands_bbands_bb_upper < close"),
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
    "start_date": datetime.datetime(2024, 10, 1, 0, 0),
    "end_date": datetime.datetime(2025, 2, 26, 0, 0),
    "rules": None,
    "symbol": "BTCUSDT",
    "exchange": "binance",
}

if __name__ == "__main__":
    res = run_backtest(strategy)
    pprint.pprint(res.get("summary"))
