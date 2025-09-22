import datetime
import numpy as np
import pandas as pd
from fast_trade.calculate_perc_missing import calculate_perc_missing


def test_calculate_perc_missing_none_missing():
    # generate a list of dates from the first to the last date in the dataframe
    today = datetime.datetime.now()
    last_week = today - datetime.timedelta(days=7)
    mock_dates = pd.date_range(start=last_week, end=today, freq="1Min")
    mock_df = pd.DataFrame(index=mock_dates)
    mock_df["close"] = 0
    mock_df = mock_df.asfreq("1Min")
    [perc_missinng, total_missing] = calculate_perc_missing(mock_df)

    assert perc_missinng == 0.0
    assert total_missing == 0.0
    print("✓ test_calculate_perc_missing_none_missing passed")


def test_calculate_perc_missing_some_missing():
    today = datetime.datetime.now().replace(second=0, microsecond=0, hour=12, minute=0)
    last_week = today - datetime.timedelta(hours=1)
    mock_dates = pd.date_range(start=last_week, freq="1Min", periods=10)
    mock_df = pd.DataFrame(index=mock_dates)
    mock_df = mock_df.asfreq("1Min")

    # remove 2 dates from the list
    mock_df = mock_df.drop(mock_df.index[-5])
    mock_df = mock_df.drop(mock_df.index[-5])
    mock_df["close"] = 0

    [perc_missinng, total_missing] = calculate_perc_missing(mock_df)

    assert perc_missinng == 20
    assert total_missing == 2
    print("✓ test_calculate_perc_missing_some_missing passed")


def test_calculate_perc_missing_empty_df():
    mock_df = pd.DataFrame()
    try:
        calculate_perc_missing(mock_df)
        assert False, "Should have raised ValueError"
    except ValueError:
        print("✓ test_calculate_perc_missing_empty_df passed")


def test_calculate_perc_missing_no_index():
    mock_df = pd.DataFrame({"close": [1, 2, 3, 4, 5]})
    try:
        calculate_perc_missing(mock_df)
        assert False, "Should have raised ValueError"
    except ValueError:
        print("✓ test_calculate_perc_missing_no_index passed")


def test_calculate_perc_missing_with_different_freq():
    """Test that the function correctly infers frequency from data with missing values"""
    today = datetime.datetime.now().replace(second=0, microsecond=0, hour=12, minute=0)
    last_week = today - datetime.timedelta(hours=1)

    # Create data at 10-minute intervals
    mock_dates = pd.date_range(start=last_week, freq="10Min", periods=6)
    # This creates: 11:00, 11:10, 11:20, 11:30, 11:40, 11:50

    # Remove middle points to create missing data
    mock_dates = mock_dates.delete([2, 3])  # Remove 11:20 and 11:30
    # Now we have: 11:00, 11:10, 11:40, 11:50

    mock_df = pd.DataFrame(index=mock_dates)
    mock_df["close"] = 0

    [perc_missinng, total_missing] = calculate_perc_missing(mock_df)

    # Expected: 6 total points (11:00 to 11:50 every 10 min)
    # Actual: 4 points
    # Missing: 2 points
    # Percentage: (2/6) * 100 = 33.33%

    assert abs(perc_missinng - 33.33) < 0.1  # Allow small floating point difference
    assert total_missing == 2
    print(
        f"✓ test_calculate_perc_missing_with_different_freq passed: {perc_missinng}% missing, {total_missing} total missing"
    )


def test_calculate_perc_missing_infer_5min_freq():
    """Test that 5-minute frequency is correctly inferred"""
    today = datetime.datetime.now().replace(second=0, microsecond=0, hour=12, minute=0)

    # Create data at 5-minute intervals
    mock_dates = pd.date_range(
        start=today - datetime.timedelta(minutes=30), freq="5Min", periods=7
    )
    mock_df = pd.DataFrame(index=mock_dates)
    mock_df["close"] = 0

    # Remove one point to create missing data
    mock_df = mock_df.drop(mock_df.index[3])

    [perc_missinng, total_missing] = calculate_perc_missing(mock_df)

    # Expected: 7 total points over 30 minutes (every 5 min)
    # Actual: 6 points
    # Missing: 1 point
    # Percentage: (1/7) * 100 ≈ 14.29%

    assert abs(perc_missinng - 14.29) < 0.1
    assert total_missing == 1
    print(
        f"✓ test_calculate_perc_missing_infer_5min_freq passed: {perc_missinng}% missing, {total_missing} total missing"
    )
