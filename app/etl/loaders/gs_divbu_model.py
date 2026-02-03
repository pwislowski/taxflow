from typing import Dict, List, Optional

import pandas as pd

from app.config import SharedServicesConfig
from app.enums import (
    GsDivBuExpenseColumns,
    GsDivBuIncomeColumns,
    HighLevelSegmentedPnlColumns,
)
from app.interfaces import GsEtlLoader

from ..base import BaseETL
from ..standardize import standardize_cols_to_float, standardize_cols_to_str

EXPENSE_COLUMN_ORDER = GsDivBuExpenseColumns.list_column_order()

INCOME_COLUMN_ORDER = GsDivBuIncomeColumns.list_column_order()

HL_PNL_COLUMN_ORDER = HighLevelSegmentedPnlColumns.list_column_order()


def transform_gs_divbu_expense_df(
    df: pd.DataFrame,
    columns_order: List[GsDivBuExpenseColumns] = EXPENSE_COLUMN_ORDER,
) -> pd.DataFrame:
    text_cols = [columns_order[0]]
    numeric_cols = columns_order[1:]

    dfc = df.copy()
    dfc.columns = columns_order

    dfc = standardize_cols_to_str(dfc, text_cols)  # type:ignore
    dfc = standardize_cols_to_float(dfc, numeric_cols)  # type:ignore

    return dfc


def transform_gs_divbu_income_df(
    df: pd.DataFrame,
    columns_order: List[GsDivBuIncomeColumns] = INCOME_COLUMN_ORDER,
) -> pd.DataFrame:
    text_cols = [columns_order[0]]
    numeric_cols = columns_order[1:]

    dfc = df.copy()
    dfc.columns = columns_order

    dfc = standardize_cols_to_str(dfc, text_cols)  # type:ignore
    dfc = standardize_cols_to_float(dfc, numeric_cols)  # type:ignore

    return dfc


def transform_gs_divbu_model_df(
    df_gs_income: pd.DataFrame, df_divbu_income: pd.DataFrame
) -> pd.DataFrame:
    df_gs_income = df_gs_income[
        [
            GsDivBuIncomeColumns.CompanyCode,
            GsDivBuIncomeColumns.NetExpense,
            GsDivBuIncomeColumns.NetIncome,
        ]
    ].set_index(GsDivBuIncomeColumns.CompanyCode)

    df_divbu_income = df_divbu_income[
        [
            GsDivBuIncomeColumns.CompanyCode,
            GsDivBuIncomeColumns.NetExpense,
            GsDivBuIncomeColumns.NetIncome,
        ]
    ].set_index(GsDivBuIncomeColumns.CompanyCode)

    df = pd.merge(
        df_gs_income,
        df_divbu_income,
        how="outer",
        on=GsDivBuIncomeColumns.CompanyCode,
        suffixes=["_gs", "_divbu"],  # type:ignore
    ).fillna(0)

    df[HighLevelSegmentedPnlColumns.TotalNetSales] = (
        df[GsDivBuIncomeColumns.NetIncome + "_gs"]
        + df[GsDivBuIncomeColumns.NetIncome + "_divbu"]
    ) / 10**3
    df[HighLevelSegmentedPnlColumns.GrossProfitAfterVariances] = df[
        HighLevelSegmentedPnlColumns.TotalNetSales
    ]
    df[HighLevelSegmentedPnlColumns.TotalSAR] = (
        (
            df[GsDivBuIncomeColumns.NetExpense + "_gs"]
            + df[GsDivBuIncomeColumns.NetExpense + "_divbu"]
        )
        / 10**3
        * -1
    )
    df[HighLevelSegmentedPnlColumns.TotalEBIT] = (
        df[HighLevelSegmentedPnlColumns.GrossProfitAfterVariances]
        + df[HighLevelSegmentedPnlColumns.TotalSAR]
    )

    return df[HL_PNL_COLUMN_ORDER]  # type:ignore


class GsDivbuModelETL(GsEtlLoader):
    def __init__(self, config: SharedServicesConfig) -> None:
        self.config = config
        self.df_divbu_income: Optional[pd.DataFrame] = None
        self.df_divbu_expense: Optional[pd.DataFrame] = None
        self.df_gs_income: Optional[pd.DataFrame] = None
        self.df_gs_expense: Optional[pd.DataFrame] = None
        self.data_ok: Optional[pd.DataFrame] = None

    def load_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        self.df_divbu_income = BaseETL(
            self.config.divbu_income.fpath, self.config.divbu_income.sheet_name
        ).load()
        self.df_divbu_expense = BaseETL(
            self.config.divbu_expense.fpath, self.config.divbu_expense.sheet_name
        ).load()
        self.df_gs_income = BaseETL(
            self.config.gs_income.fpath, self.config.gs_income.sheet_name
        ).load()
        self.df_gs_expense = BaseETL(
            self.config.gs_expense.fpath, self.config.gs_expense.sheet_name
        ).load()

        # preprocessed data
        self.df_divbu_expense = self._transform_expense_df(self.df_divbu_expense)
        self.df_gs_expense = self._transform_expense_df(self.df_gs_expense)

        self.df_divbu_income = self._transform_income_df(self.df_divbu_income)
        self.df_gs_income = self._transform_income_df(self.df_gs_income)

        return self.df_gs_income, self.df_divbu_income

    def _transform_expense_df(
        self,
        df: pd.DataFrame,
        columns_order: List[GsDivBuExpenseColumns] = EXPENSE_COLUMN_ORDER,
    ) -> pd.DataFrame:
        return transform_gs_divbu_expense_df(df, columns_order)

    def _transform_income_df(
        self,
        df: pd.DataFrame,
        columns_order: List[GsDivBuIncomeColumns] = INCOME_COLUMN_ORDER,
    ) -> pd.DataFrame:
        return transform_gs_divbu_income_df(df, columns_order)

    def transform(
        self,
        df_gs_income: pd.DataFrame | None = None,
        df_divbu_income: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        "Applies to income tables only"
        if df_gs_income is None or df_divbu_income is None:
            df_gs_income, df_divbu_income = self.load_data()

        self.data_ok = transform_gs_divbu_model_df(df_gs_income, df_divbu_income)

        return self.data_ok

    def generate_pipeline_context(
        self,
    ) -> Dict[str, Dict[HighLevelSegmentedPnlColumns, float]]:
        ret = {}

        assert isinstance(self.data_ok, pd.DataFrame)

        records = self.data_ok.reset_index().to_dict(orient="records")

        for record in records:
            ret[record[GsDivBuIncomeColumns.CompanyCode]] = record

        return ret
