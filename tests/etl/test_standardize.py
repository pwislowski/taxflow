import pandas as pd
import pytest

from app.enums import HighLevelSegmentedPnlColumns, SapBwColumns
from app.etl.standardize import (
    standardize_cols_to_float,
    standardize_cols_to_str,
    standardize_from_pct_to_float,
    standardize_legacy_columns,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("TOTAL NET SALES", HighLevelSegmentedPnlColumns.TotalNetSales),
        ("EBIT", HighLevelSegmentedPnlColumns.TotalEBIT),
        (
            "TOTAL GROSS PROFIT BEFORE VARIANCES",
            HighLevelSegmentedPnlColumns.GrossProfitBeforeVariances,
        ),
        (
            "TOTAL GROSS PROFIT AFTER VARIANCES",
            HighLevelSegmentedPnlColumns.GrossProfitAfterVariances,
        ),
        ("TOTAL SAR", HighLevelSegmentedPnlColumns.TotalSAR),
        ("TOTAL OPERATIONAL INCOME", HighLevelSegmentedPnlColumns.OpertionalIncome),
        ("TOTAL COGS (3rd PARTIES + GC)", HighLevelSegmentedPnlColumns.Cogs),
    ],
)
def test_standardize_legacy_columns_maps_known_labels(raw, expected):
    assert standardize_legacy_columns(raw) == expected


def test_standardize_legacy_columns_rejects_unknown_label():
    with pytest.raises(ValueError, match="Invalid column name"):
        standardize_legacy_columns("UNKNOWN")


def test_standardize_cols_to_str_converts_expected_columns():
    df = pd.DataFrame(
        {
            SapBwColumns.CompanyCode: [1, 2],
            SapBwColumns.CompanyCodeText: ["A", "B"],
            "other": [3, 4],
        }
    )

    result = standardize_cols_to_str(
        df, [SapBwColumns.CompanyCode, SapBwColumns.CompanyCodeText]
    )

    assert result[SapBwColumns.CompanyCode].dtype == object
    assert result[SapBwColumns.CompanyCode].tolist() == ["1", "2"]
    assert result[SapBwColumns.CompanyCodeText].tolist() == ["A", "B"]


def test_standardize_cols_to_str_missing_column_raises():
    df = pd.DataFrame({SapBwColumns.CompanyCode: [1]})

    with pytest.raises(KeyError, match="not found in df"):
        standardize_cols_to_str(df, [SapBwColumns.CompanyCodeText])


def test_standardize_cols_to_float_converts_values_and_fills_na():
    df = pd.DataFrame({SapBwColumns.Amount: [1, None, 3]})

    result = standardize_cols_to_float(df, [SapBwColumns.Amount])

    assert result[SapBwColumns.Amount].tolist() == [1.0, 0.0, 3.0]


def test_standardize_cols_to_float_rejects_non_numeric():
    df = pd.DataFrame({SapBwColumns.Amount: ["nope"]})

    with pytest.raises(ValueError, match="failed to be transformed"):
        standardize_cols_to_float(df, [SapBwColumns.Amount])


def test_standardize_from_pct_to_float_converts_percentages():
    df = pd.DataFrame({"pct": ["50%", "12.5%", None]})

    result = standardize_from_pct_to_float(df, ["pct"])

    assert result["pct"].tolist() == [0.5, 0.125, 0.0]


def test_standardize_from_pct_to_float_missing_column_raises():
    df = pd.DataFrame({"pct": ["50%"]})

    with pytest.raises(KeyError, match="not found in df"):
        standardize_from_pct_to_float(df, ["missing"])
