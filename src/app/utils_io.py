"""I/O utility functions."""

from pathlib import Path
import pandas as pd


def write_csv(df: pd.DataFrame, path: str) -> None:
    """Write a pandas DataFrame to CSV ensuring the parent directory exists.

    The file will be saved with UTF‑8 BOM encoding so that spreadsheet software
    such as Excel can correctly interpret non‑ASCII characters.
    """
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")