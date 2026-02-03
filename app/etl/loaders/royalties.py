from pathlib import Path
from typing import TYPE_CHECKING, Sequence

import pandas as pd

from app.config import RoyaltiesConfig
from app.enums import SapBwColumns
from app.etl.standardize import standardize_cols_to_str, standardize_legacy_columns

from ..base import BaseETL


def transform_royalties_df(df: pd.DataFrame, accounts: Sequence[str]) -> pd.DataFrame:
    text_columns = [
        SapBwColumns.PnlItem,
        SapBwColumns.CompanyCode,
        SapBwColumns.CompanyCodeText,
        SapBwColumns.ProfitCenter,
        SapBwColumns.ProfitCenterText,
        SapBwColumns.GlAccount,
        SapBwColumns.GlAccountText,
        SapBwColumns.MprColumn,
    ]

    value_columns = [SapBwColumns.Amount]

    columns = [*text_columns, *value_columns]

    dfc = df.copy()
    dfc.columns = columns

    dfc = standardize_cols_to_str(dfc, text_columns)
    dfc[SapBwColumns.PnlItem] = dfc[SapBwColumns.PnlItem].apply(
        standardize_legacy_columns
    )

    dfc = dfc[dfc[SapBwColumns.GlAccount].isin(accounts)]

    if TYPE_CHECKING:
        assert isinstance(dfc, pd.DataFrame)

    dfc = (
        dfc.groupby(
            by=[
                SapBwColumns.PnlItem,
                SapBwColumns.CompanyCode,
                SapBwColumns.GlAccount,
            ]
        )[SapBwColumns.Amount]
        .sum()
        .reset_index()
    )

    if TYPE_CHECKING:
        assert isinstance(dfc, pd.DataFrame)

    return dfc


class RoyaltiesETL(BaseETL):
    """ETL loader for royalties data from SAP BW export.

    This class loads the royalties sheet from an Excel file, standardizes column names
    and types, converting text columns to strings.

    Attributes:
        data: The raw loaded DataFrame before transformation.
    """

    def __init__(
        self, config: RoyaltiesConfig, fname: str | Path, sheet_name: str | None = None
    ) -> None:
        super().__init__(fname, sheet_name)
        self.data: pd.DataFrame | None = None
        self.config = config

    def load_data(self) -> pd.DataFrame:
        self.data = self.load()
        return self.data

    def transform(self, df: pd.DataFrame | None = None) -> pd.DataFrame:
        """Transform the royalties data by standardizing columns.

        Renames columns according to SapBwColumns and standardizes text columns to str.

        Returns:
            pd.DataFrame: Transformed DataFrame with standardized columns.

        Raises:
            Any exceptions from BaseETL methods or pandas operations.
        """
        dfc = df if df is not None else self.load_data()
        return transform_royalties_df(dfc, self.config.accounts_in_scope)
