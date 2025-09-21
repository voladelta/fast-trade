import pandas as pd


def _infer_freq_from_index(index):
    """
    Infer the most likely time frequency from a DatetimeIndex by finding the smallest gap.

    Parameters:
    - index (pd.DatetimeIndex): The DatetimeIndex to analyze

    Returns:
    - str: The inferred frequency string (e.g., '1Min', '5Min', '1H', etc.)
    """
    if len(index) < 2:
        return "1Min"  # Default for single data point

    # Calculate time deltas between consecutive points
    deltas = index[1:] - index[:-1]

    # Use the minimum delta (smallest gap) to infer frequency
    # This works better than mode for sparse data with missing points
    min_delta = deltas.min()

    # Convert timedelta to frequency string
    total_seconds = min_delta.total_seconds()

    if total_seconds >= 86400:  # Day or more
        days = total_seconds // 86400
        return f"{int(days)}D"
    elif total_seconds >= 3600:  # Hour or more
        hours = total_seconds // 3600
        return f"{int(hours)}H"
    elif total_seconds >= 60:  # Minute or more
        minutes = total_seconds // 60
        return f"{int(minutes)}Min"
    else:  # Less than a minute
        return "1Min"  # Default to 1 minute


def calculate_perc_missing(df):
    """
    Calculate the percentage and total count of missing entries
    in a DataFrame based on a time-index.

    Parameters:
    - df (pd.DataFrame): The input DataFrame with a DatetimeIndex.

    Returns:
    - list: [percentage_missing (float), total_missing (float)]
    """
    # Handle empty DataFrame
    if df.empty:
        raise ValueError("DataFrame is empty")

    # Get the original frequency and reindex with it
    # check if the index is a DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("DataFrame index is not a DatetimeIndex")
    freq = df.index.freq
    if freq is None:
        freq = _infer_freq_from_index(df.index)
    else:
        # Convert DateOffset to string if needed
        freq = df.index.freqstr
    # Get the full range of expected dates
    start = df.index.min()
    end = df.index.max()
    expected_index = pd.date_range(start=start, end=end, freq=freq)

    # Calculate missing values
    total_possible = len(expected_index)
    total_actual = len(df)

    total_missing = total_possible - total_actual

    # Calculate percentage
    perc_missing = (total_missing / total_possible) * 100 if total_possible > 0 else 0.0
    perc_missing = round(perc_missing, 2)
    return [perc_missing, 0 if total_missing < 0 else total_missing]
