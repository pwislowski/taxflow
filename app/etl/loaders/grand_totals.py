from pathlib import Path

import pandas as pd

from app.enums import SapBwColumns

from ..base import BaseETL
from ..standardize import standardize_cols_to_str, standardize_legacy_columns


def transform_grand_totals_df(df: pd.DataFrame) -> pd.DataFrame:
    dfc = df.copy()
    cols = list(dfc.columns)
    dfc = dfc.melt(id_vars=cols[:3], value_vars=cols[3:], var_name="values")

    text_columns = [
        SapBwColumns.CompanyCode,
        SapBwColumns.CompanyCodeText,
        SapBwColumns.MprColumn,
    ]

    value_columns = [SapBwColumns.PnlItem, SapBwColumns.Amount]

    columns = [*text_columns, *value_columns]

    dfc.columns = columns
    dfc = standardize_cols_to_str(dfc, text_columns)
    dfc[SapBwColumns.PnlItem] = dfc[SapBwColumns.PnlItem].apply(
        standardize_legacy_columns
    )

    # remove first row
    dfc = dfc.iloc[1:]
    dfc.reset_index(inplace=True, drop=True)

    dfc = dfc[dfc[SapBwColumns.CompanyCode] != "Company code"]

    return dfc


class GrandTotalETL(BaseETL):
    """ETL loader for grand totals data from SAP BW export.

    This class loads the grand totals sheet from an Excel file, melts the wide-format data
    into long format, standardizes column types, and removes header rows.

    Attributes:
        data: The raw loaded DataFrame before transformation.
    """

    def __init__(self, fname: str | Path, sheet_name: str | None = None) -> None:
        super().__init__(fname, sheet_name)
        self.data: pd.DataFrame | None = None

    def load_data(self) -> pd.DataFrame:
        self.data = self.load()
        return self.data

    def transform(self, df: pd.DataFrame | None = None) -> pd.DataFrame:
        """Transform the grand totals data by melting columns and standardizing types.

        Reshapes the DataFrame from wide to long format using melt on value columns,
        renames columns according to SapBwColumns, standardizes text columns to str,
        and skips the first row (likely a header).

        Returns:
            pd.DataFrame: Transformed long-format DataFrame with standardized columns.

        Raises:
            Any exceptions from BaseETL methods or pandas operations.
        """
        dfc = df if df is not None else self.load_data()
        return transform_grand_totals_df(dfc)
