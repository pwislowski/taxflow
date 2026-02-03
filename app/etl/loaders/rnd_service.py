from typing import TYPE_CHECKING, Sequence

import pandas as pd

from app.config import RndServicesConfig
from app.enums import HighLevelSegmentedPnlColumns, SapBwColumns
from app.interfaces import EtlLoader

from ..base import BaseETL
from ..standardize import standardize_cols_to_str, standardize_legacy_columns


def transform_rnd_fr09_df(
    df: pd.DataFrame, accounts_in_scope: Sequence[int] | None = None
) -> pd.DataFrame:
    text_columns = [
        SapBwColumns.CompanyCode,
        SapBwColumns.CompanyCodeText,
        SapBwColumns.PnlItem,
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

    if accounts_in_scope:
        dfc = dfc[dfc[SapBwColumns.GlAccount].isin(accounts_in_scope)]

    if TYPE_CHECKING:
        assert isinstance(dfc, pd.DataFrame)

    dfc = (
        dfc.groupby(
            by=[
                SapBwColumns.CompanyCode,
                SapBwColumns.PnlItem,
            ]
        )[SapBwColumns.Amount]
        .sum()
        .reset_index()
    )

    dfc[SapBwColumns.PnlItem] = dfc[SapBwColumns.PnlItem].map(
        lambda x: {
            "EBIT": HighLevelSegmentedPnlColumns.TotalEBIT,
            "TOTAL COGS (3rd PARTIES + GC)": HighLevelSegmentedPnlColumns.Cogs,
            "TOTAL GROSS PROFIT AFTER VARIANCES": HighLevelSegmentedPnlColumns.GrossProfitAfterVariances,
            "TOTAL GROSS PROFIT BEFORE VARIANCES": HighLevelSegmentedPnlColumns.GrossProfitBeforeVariances,
            "TOTAL NET SALES": HighLevelSegmentedPnlColumns.TotalNetSales,
            "TOTAL SAR": HighLevelSegmentedPnlColumns.TotalSAR,
        }.get(x)
    )

    if TYPE_CHECKING:
        assert isinstance(dfc, pd.DataFrame)

    return dfc


def transform_rnd_de03_df(
    df: pd.DataFrame,
    company_code: str,
    accounts_in_scope: Sequence[int] | None = None,
) -> pd.DataFrame:
    text_columns = [
        SapBwColumns.CompanyCode,
        SapBwColumns.CompanyCodeText,
        SapBwColumns.PnlItem,
        SapBwColumns.LocalCurrency,
        SapBwColumns.LocalCurrencyText,
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

    dfc[SapBwColumns.PnlItem] = dfc[SapBwColumns.PnlItem].map(
        lambda x: {
            "EBIT": HighLevelSegmentedPnlColumns.TotalEBIT,
            "TOTAL COGS (3rd PARTIES + GC)": HighLevelSegmentedPnlColumns.Cogs,
            "TOTAL GROSS PROFIT AFTER VARIANCES": HighLevelSegmentedPnlColumns.GrossProfitAfterVariances,
            "TOTAL GROSS PROFIT BEFORE VARIANCES": HighLevelSegmentedPnlColumns.GrossProfitBeforeVariances,
            "TOTAL NET SALES": HighLevelSegmentedPnlColumns.TotalNetSales,
            "TOTAL SAR": HighLevelSegmentedPnlColumns.TotalSAR,
            "OH Research & Development": HighLevelSegmentedPnlColumns.TotalSAR,
        }.get(x)
    )

    accounts = accounts_in_scope if accounts_in_scope else []

    revenue: float = dfc[  # type:ignore
        (dfc[SapBwColumns.ProfitCenter] == "7")
        & (dfc[SapBwColumns.PnlItem] == HighLevelSegmentedPnlColumns.TotalNetSales)
        & (dfc[SapBwColumns.GlAccount].isin(accounts))
    ][SapBwColumns.Amount].sum()

    sar_cost: float = dfc[  # type:ignore
        (dfc[SapBwColumns.ProfitCenter] == "39")
        & (dfc[SapBwColumns.PnlItem] == HighLevelSegmentedPnlColumns.TotalSAR)
    ][SapBwColumns.Amount].sum()

    rows = [
        HighLevelSegmentedPnlColumns.TotalNetSales,
        HighLevelSegmentedPnlColumns.Cogs,
        HighLevelSegmentedPnlColumns.GrossProfitBeforeVariances,
        HighLevelSegmentedPnlColumns.GrossProfitAfterVariances,
        HighLevelSegmentedPnlColumns.TotalSAR,
        HighLevelSegmentedPnlColumns.TotalEBIT,
    ]
    data = {
        SapBwColumns.CompanyCode: [company_code] * len(rows),
        SapBwColumns.PnlItem: rows,
        SapBwColumns.Amount: [
            revenue,
            0,
            revenue,
            revenue,
            sar_cost,
            revenue + sar_cost,
        ],
    }

    if TYPE_CHECKING:
        assert isinstance(dfc, pd.DataFrame)

    return pd.DataFrame(data)


class RndServiceETL(EtlLoader):
    """ETL loader for R&D data from SAP BW export.

    This class loads the R&D sheet from an Excel file, standardizes column names
    and types, converting text columns to strings.

    Attributes:
        data: The raw loaded DataFrame before transformation.
    """

    def __init__(self, configs: Sequence[RndServicesConfig]) -> None:
        self.configs = configs

    def transform_fr09(
        self, conf: RndServicesConfig, df: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        """Transform the R&D data by standardizing columns.

        Renames columns according to SapBwColumns and standardizes text columns to str.

        Returns:
            pd.DataFrame: Transformed DataFrame with standardized columns.

        Raises:
            Any exceptions from BaseETL methods or pandas operations.
        """
        if df is None:
            base_etl = BaseETL(fname=conf.fpath, sheet_name=conf.sheet_name)
            df = base_etl.load()

        return transform_rnd_fr09_df(df, conf.accounts_in_scope)

    def transform_de03(
        self, conf: RndServicesConfig, df: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        """Transform the R&D data by standardizing columns.

        Renames columns according to SapBwColumns and standardizes text columns to str.

        Returns:
            pd.DataFrame: Transformed DataFrame with standardized columns.

        Raises:
            Any exceptions from BaseETL methods or pandas operations.
        """
        if df is None:
            base_etl = BaseETL(conf.fpath, conf.sheet_name)
            df = base_etl.load()

        return transform_rnd_de03_df(df, conf.company_code, conf.accounts_in_scope)

    def transform(self) -> pd.DataFrame:
        df = pd.DataFrame()

        for conf in self.configs:
            df_ok = pd.DataFrame()
            match conf.company_code:
                case "DE03":
                    df_ok = self.transform_de03(conf)
                case "FR09":
                    df_ok = self.transform_fr09(conf)
            df = pd.concat([df, df_ok], axis=0)

        return df
