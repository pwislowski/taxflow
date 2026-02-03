from typing import Sequence

import pandas as pd

from app.enums import HighLevelSegmentedPnlColumns, SapBwColumns


def standardize_legacy_columns(value: str) -> HighLevelSegmentedPnlColumns:
    match value:
        case "TOTAL NET SALES":
            return HighLevelSegmentedPnlColumns.TotalNetSales

        case "EBIT":
            return HighLevelSegmentedPnlColumns.TotalEBIT

        case "TOTAL GROSS PROFIT BEFORE VARIANCES":
            return HighLevelSegmentedPnlColumns.GrossProfitBeforeVariances

        case "TOTAL GROSS PROFIT AFTER VARIANCES":
            return HighLevelSegmentedPnlColumns.GrossProfitAfterVariances

        case "TOTAL SAR":
            return HighLevelSegmentedPnlColumns.TotalSAR

        case "TOTAL OPERATIONAL INCOME":
            return HighLevelSegmentedPnlColumns.OpertionalIncome

        case "TOTAL COGS (3rd PARTIES + GC)":
            return HighLevelSegmentedPnlColumns.Cogs

        case _:
            raise ValueError(f"Invalid column name: {value}")


def standardize_cols_to_str(
    df: pd.DataFrame, columns: Sequence[SapBwColumns | str]
) -> pd.DataFrame:
    """Standardize specified columns in the DataFrame to string type."""
    dfc = df.copy()

    ok = all([col in dfc.columns for col in columns])

    if not ok:
        err_msg = ""

        for col in columns:
            if col not in dfc.columns:
                msg = f"[String Standarizing] Column - `{col}` - not found in df"
                err_msg += msg + "\n"

        raise KeyError(err_msg)

    for col in columns:
        dfc[col] = dfc[col].astype(str)

    return dfc


def standardize_cols_to_float(
    df: pd.DataFrame, columns: Sequence[SapBwColumns | str]
) -> pd.DataFrame:
    """Standardize specified columns in the DataFrame to float type."""
    dfc = df.copy()

    ok = all([col in dfc.columns for col in columns])

    if not ok:
        err_msg = ""

        for col in columns:
            if col not in dfc.columns:
                msg = f"[Numeric Standarizing] Column - `{col}` - not found in df"
                err_msg += msg + "\n"

        raise KeyError(err_msg)

    for col in columns:
        try:
            dfc[col] = dfc[col].fillna(0).astype(float)
        except ValueError as e:
            raise ValueError(
                f"[Numeric Standarizing] Column - `{col}` - failed to be transformed"
            ) from e

    return dfc


def standardize_from_pct_to_float(
    df: pd.DataFrame, columns: Sequence[str]
) -> pd.DataFrame:
    """Convert percentage columns from string format (e.g., '50%') to float (0.5)."""
    dfc = df.copy()
    ok = all([col in dfc.columns for col in columns])

    if not ok:
        err_msg = ""

        for col in columns:
            if col not in dfc.columns:
                msg = f"[Numeric Standarizing] Column - `{col}` - not found in df"
                err_msg += msg + "\n"

        raise KeyError(err_msg)

    for col in columns:
        dfc[col] = (
            dfc[col].fillna("0%").astype(str).str.replace("%", "").astype(float) / 100
        )

    return dfc
