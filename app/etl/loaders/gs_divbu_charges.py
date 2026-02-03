from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional, Sequence

import pandas as pd

from app.config import GsChargesConfig
from app.enums import HighLevelSegmentedPnlColumns, SapBwColumns
from app.interfaces import GsEtlLoader

from ..base import BaseETL
from ..standardize import standardize_cols_to_float, standardize_cols_to_str


def transform_gs_divbu_charges_df(
    df: pd.DataFrame, accounts: Sequence[str]
) -> pd.DataFrame:
    text_cols = [
        SapBwColumns.PnlItem,
        SapBwColumns.FiscalYearPeriod,
        SapBwColumns.FiscalYearPeriodText,
        SapBwColumns.CompanyCode,
        SapBwColumns.CompanyCodeText,
        SapBwColumns.ProfitCenter,
        SapBwColumns.ProfitCenterText,
        SapBwColumns.GlAccount,
        SapBwColumns.GlAccountText,
        SapBwColumns.MprColumn,
    ]

    numeric_cols = [SapBwColumns.Amount]

    columns = [*text_cols, *numeric_cols]

    dfc = df.copy()
    dfc.columns = columns

    dfc = standardize_cols_to_str(dfc, text_cols)
    dfc = standardize_cols_to_float(dfc, numeric_cols)

    dfc = dfc[dfc[SapBwColumns.GlAccount].isin(accounts)]
    by = [SapBwColumns.CompanyCode, SapBwColumns.PnlItem]
    dfc = dfc.groupby(by=by)[SapBwColumns.Amount].sum().reset_index()

    if TYPE_CHECKING:
        assert isinstance(dfc, pd.DataFrame)

    dfc = dfc.pivot_table(
        values=SapBwColumns.Amount,
        columns=SapBwColumns.PnlItem,
        index=SapBwColumns.CompanyCode,
    )

    dfc = dfc.rename(
        columns={
            "EBIT": HighLevelSegmentedPnlColumns.TotalEBIT,
            "TOTAL GROSS PROFIT AFTER VARIANCES": HighLevelSegmentedPnlColumns.GrossProfitAfterVariances,
            "TOTAL NET SALES": HighLevelSegmentedPnlColumns.TotalNetSales,
            "TOTAL SAR": HighLevelSegmentedPnlColumns.TotalSAR,
        }
    )

    dfc = dfc.fillna(0)

    return dfc


class GsDivBuChargesETL(BaseETL, GsEtlLoader):
    def __init__(
        self,
        config: GsChargesConfig,
        fname: str | Path,
        sheet_name: str | None = None,
    ) -> None:
        super().__init__(fname, sheet_name)
        self.data: pd.DataFrame | None = None
        self.config = config
        self.data_ok: Optional[pd.DataFrame] = None

    def load_data(self) -> pd.DataFrame:
        self.data = self.load()
        return self.data

    def transform(self, df: pd.DataFrame | None = None) -> pd.DataFrame:
        dfc = df if df is not None else self.load_data()
        df_ok = transform_gs_divbu_charges_df(dfc, self.config.accounts_in_scope)
        self.data_ok = df_ok

        return df_ok

    def generate_pipeline_context(
        self,
    ) -> Dict[str, Dict[HighLevelSegmentedPnlColumns, float]]:
        ret = {}

        assert isinstance(self.data_ok, pd.DataFrame)

        records = self.data_ok.reset_index().to_dict(orient="records")

        for record in records:
            ret[record[SapBwColumns.CompanyCode]] = record

        return ret
