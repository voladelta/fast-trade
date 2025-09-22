# flake8: noqa
import argparse
import datetime
import json
import os
import sys
from pprint import pprint

import matplotlib.pyplot as plt

from fast_trade.archive.cli import download_asset, get_assets
from fast_trade.archive.update_archive import update_archive
from fast_trade.validate_backtest import validate_backtest

from .cli_helpers import _apply_mods, create_plot, open_strat_file, save
from .run_backtest import run_backtest

parser = argparse.ArgumentParser(
    description="Fast Trade CLI",
    prog="ft",
)

sub_parsers = parser.add_subparsers()

# build the argement parser downloading stuff
default_end_date = datetime.datetime.now(datetime.timezone.utc)
default_start_date = default_end_date - datetime.timedelta(days=30)

download_parser = sub_parsers.add_parser("download", help="download data")
download_parser.add_argument("symbol", help="symbol to download", type=str)
download_parser.add_argument(
    "exchange",
    help="Which exchange to download data from. Defaults to binance.com",
    type=str,
    default="binance",
    choices=["binance", "coinbase"],
)
download_parser.add_argument(
    "--start",
    help="Date to start downloading data from. Defaults to 30 days ago.",
    type=str,
    default=default_start_date.isoformat(),
)

download_parser.add_argument(
    "--end",
    help="Date to end downloading data. Defaults to today.",
    type=str,
    default=default_end_date.isoformat(),
)


backtest_parser = sub_parsers.add_parser("backtest", help="backtest a strategy")
backtest_parser.add_argument(
    "strategy",
    help="path to strategy file",
    type=str,
)

backtest_parser.add_argument(
    "--mods", help="Modifiers for strategy/backtest", nargs="*"
)
backtest_parser.add_argument(
    "--save",
    help="save the backtest results to a directory",
    action="store_true",
    default=False,
)
backtest_parser.add_argument(
    "--plot",
    help="plot the backtest results",
    action="store_true",
    default=False,
)

validate_backtest_parser = sub_parsers.add_parser(
    "validate", help="validate a strategy file"
)

validate_backtest_parser.add_argument(
    "strategy",
    help="path to strategy file",
    type=str,
)
validate_backtest_parser.add_argument(
    "--mods", help="Modifiers for strategy/backtest", nargs="*"
)

get_assets_parser = sub_parsers.add_parser("assets", help="get assets")
get_assets_parser.add_argument(
    "--exchange",
    help="",
    type=str,
    default="local",
    choices=["local", "binance", "coinbase"],
)

update_archive_parser = sub_parsers.add_parser(
    "update_archive", help="update the archive"
)


def backtest_helper(*args, **kwargs):
    # match the mods to the kwargs

    strat_obj = open_strat_file(kwargs.get("strategy"))

    if not strat_obj:
        print("Could not open strategy file: {}".format(kwargs.get("strategy")))
        sys.exit(1)
    strat_obj = _apply_mods(strat_obj, kwargs.get("mods"))

    result = run_backtest(strat_obj)
    summary = result.get("summary")

    if kwargs.get("save"):
        save(result)

    if kwargs.get("plot"):
        create_plot(result.get("df"), result.get("trade_df"))

        plt.show()

    # convert trade duration metrics from seconds to minutes when possible
    for key in (
        "mean_trade_len",
        "max_trade_held",
        "min_trade_len",
        "median_trade_len",
    ):
        value = summary.get(key)
        summary[key] = value / 60 if isinstance(value, (int, float)) else 0

    pprint(summary)


def validate_helper(args):
    strat_obj = open_strat_file(args.get("strategy"))
    strat_obj = _apply_mods(strat_obj, args.get("mods"))

    validate_backtest(strat_obj)


command_map = {
    "download": download_asset,
    "backtest": backtest_helper,
    "validate": validate_helper,
    "assets": get_assets,
    "update_archive": update_archive,
    "-h": parser.print_help,
}


def main():
    args = parser.parse_args()
    if not len(sys.argv) > 1:
        command = "-h"
    else:
        command = sys.argv[1]

    command_func = command_map.get(command)
    if command_func is None:
        print(f"Error: Unknown command '{command}'")
        parser.print_help()
        sys.exit(1)

    command_func(**vars(args))
    print("Done running command: ", command)

    # except Exception as e:
    #     print(f"Error running command {command}: {e}")
    #     sys.exit(1)


if __name__ == "__main__":
    main()
