"""
Utility functions for reading and writing data files.

This module currently defines a simple ``write_csv`` function used by the
pipeline to write DataFrames to CSV files.
"""

import pandas as pd


def write_csv(df: pd.DataFrame, filepath: str) -> None:
    """Write a DataFrame to a CSV file with UTF-8 encoding and index omitted."""
    df.to_csv(filepath, index=False, encoding="utf-8")