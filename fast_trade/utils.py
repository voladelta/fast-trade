import re
from typing import Any, Dict

import pandas as pd


def to_dataframe(ticks: list) -> pd.DataFrame:
    """Convert list to Series compatible with the library."""

    df = pd.DataFrame(ticks)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    df.set_index("time", inplace=True)

    return df


OHLC_AGGREGATION = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum",
}


def resample(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    """Resample DataFrame by <interval>."""

    return df.resample(interval).agg(OHLC_AGGREGATION)


def resample_calendar(df: pd.DataFrame, offset: str) -> pd.DataFrame:
    """Resample the DataFrame by calendar offset.
    See http://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#anchored-offsets for compatible offsets.
    :param df: data
    :param offset: calendar offset
    :return: result DataFrame
    """

    return df.resample(offset).agg(OHLC_AGGREGATION)


def trending_up(df: pd.Series, period: int) -> pd.Series:
    """returns boolean Series if the inputs Series is trending up over last n periods.
    :param df: data
    :param period: range
    :return: result Series
    """

    return pd.Series(df.diff(period) > 0, name="trending_up {}".format(period))


def trending_down(df: pd.Series, period: int) -> pd.Series:
    """returns boolean Series if the input Series is trending up over last n periods.
    :param df: data
    :param period: range
    :return: result Series
    """

    return pd.Series(df.diff(period) < 0, name="trending_down {}".format(period))


def infer_frequency_from_index(index: pd.DatetimeIndex) -> str:
    """Infer the dominant frequency from a DatetimeIndex."""

    if not isinstance(index, pd.DatetimeIndex):
        raise ValueError("Index must be a DatetimeIndex")

    if index.freq is not None:
        return index.freqstr

    default_frequency = "1Min"

    time_diffs = index.to_series().diff()

    non_null_diffs = time_diffs.dropna()
    if non_null_diffs.empty:
        return default_frequency

    mode_result = non_null_diffs.mode()
    if mode_result.empty:
        return default_frequency

    most_common_diff = mode_result.iloc[0]
    if pd.isna(most_common_diff):
        return default_frequency

    seconds = most_common_diff.total_seconds()
    if seconds <= 0:
        return default_frequency

    if seconds < 60:
        return f"{int(seconds)}S"
    if seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes}Min"
    if seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours}H"
    days = int(seconds / 86400)
    return f"{days}D"


def infer_frequency(df: pd.DataFrame) -> str:
    """Infer frequency helper that accepts a full DataFrame."""

    return infer_frequency_from_index(df.index)


NUMERIC_PATTERN = re.compile(r"^-?\d+(?:\.\d+)?$")


def parse_logic_expr(expression: str) -> list:
    """Parse a logic expression string into array format used by the backtest system.

    Converts expressions like "rsi < 30" into ["rsi", "<", 30] and
    "bbands_bbands_bb_lower > close" into ["bbands_bbands_bb_lower", ">", "close"]

    Parameters
    ----------
    expression : str
        The logic expression to parse (e.g., "rsi < 30", "bbands_bbands_bb_lower > close")

    Returns
    -------
    list
        Array format: [field_name, operator, value] where value can be a number or field name

    Examples
    --------
    >>> parse_logic_expr("rsi < 30")
    ["rsi", "<", 30]
    >>> parse_logic_expr("bbands_bbands_bb_lower > close")
    ["bbands_bbands_bb_lower", ">", "close"]
    """
    # Pattern to match: field_name operator value
    # Where operator is one of: <, >, =, <=, >=, !=
    # And value can be a number (including negative) or another field name
    pattern = r'^([a-zA-Z_][a-zA-Z0-9_.]*)\s*([<>=]+)\s*(-?[a-zA-Z0-9_.]+(?:\.[a-zA-Z0-9_.]+)*)$'
    match = re.match(pattern, expression.strip())

    if not match:
        raise ValueError(f"Invalid logic expression format: '{expression}'. Expected format: 'field operator value'")

    field_name = match.group(1)
    operator = match.group(2)
    value_str = match.group(3)

    # Validate operator
    valid_operators = [">", "=", "<", ">=", "<="]
    if operator not in valid_operators:
        raise ValueError(f"Invalid operator '{operator}'. Valid operators are: {valid_operators}")

    # Try to convert value to number if it's numeric, otherwise treat as field name
    value = value_str
    # Check if it's a numeric value (including negative numbers)
    numeric_part = value_str.replace('.', '')
    if numeric_part.replace('-', '').isdigit() and numeric_part.count('-') <= 1 and (numeric_part.count('-') == 0 or numeric_part.startswith('-')):
        # It's a number, convert to int or float
        try:
            if '.' in value_str:
                value = float(value_str)
            else:
                value = int(value_str)
        except ValueError:
            # If conversion fails, treat as string (field name)
            pass

    return [field_name, operator, value]


def coerce_numeric_value(value: Any) -> Any:
    """Return numeric representation for strings when possible."""

    if isinstance(value, (int, float)):
        return value

    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        if value.isnumeric():
            return int(value)
        if NUMERIC_PATTERN.match(value):
            return float(value)

    return value


def extract_error_messages(error_dict: Dict[str, Any]) -> str:
    """Collect nested error messages from validation structures."""

    messages: list[str] = []

    def traverse_errors(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "msgs" and isinstance(value, list):
                    for msg in value:
                        if isinstance(msg, str):
                            messages.append(msg)
                        else:
                            traverse_errors(msg)
                else:
                    traverse_errors(value)
        elif isinstance(node, list):
            for item in node:
                traverse_errors(item)

    traverse_errors(error_dict)

    return "\n".join(messages)
