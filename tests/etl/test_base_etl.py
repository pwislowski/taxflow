import pandas as pd
import pytest

from app.etl.base import BaseETL


def test_base_etl_load_csv(tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    fpath = tmp_path / "sample.csv"
    df.to_csv(fpath, index=False)

    etl = BaseETL(fpath)
    result = etl.load()

    assert result.equals(df)


def test_base_etl_load_excel_with_sheet_name(tmp_path):
    df_sheet1 = pd.DataFrame({"a": [1], "b": ["x"]})
    df_sheet2 = pd.DataFrame({"a": [2], "b": ["y"]})
    fpath = tmp_path / "sample.xlsx"

    with pd.ExcelWriter(fpath) as writer:
        df_sheet1.to_excel(writer, sheet_name="Sheet1", index=False)
        df_sheet2.to_excel(writer, sheet_name="Sheet2", index=False)

    etl = BaseETL(fpath, sheet_name="Sheet2")
    result = etl.load()

    assert result.equals(df_sheet2)


def test_base_etl_load_excel_default_sheet(tmp_path):
    df = pd.DataFrame({"a": [10], "b": ["z"]})
    fpath = tmp_path / "sample.xlsx"

    with pd.ExcelWriter(fpath) as writer:
        df.to_excel(writer, sheet_name="First", index=False)

    etl = BaseETL(fpath)
    result = etl.load()

    assert result.equals(df)


def test_base_etl_load_rejects_unsupported_extension(tmp_path):
    fpath = tmp_path / "sample.txt"
    fpath.write_text("not a csv or excel file")

    etl = BaseETL(fpath)

    with pytest.raises(ValueError, match="not supported"):
        etl.load()
