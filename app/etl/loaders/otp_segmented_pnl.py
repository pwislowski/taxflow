from collections import defaultdict
from pathlib import Path
from typing import Dict, List

import pandas as pd
from pandas._libs.missing import NAType

from app.config import SegmentConfig
from app.enums import Activity, Category, OtpSegmentedPnlColumns
from app.etl.standardize import (
    standardize_cols_to_float,
    standardize_cols_to_str,
    standardize_from_pct_to_float,
)

from ..base import BaseETL


def transform_otp_segmented_pnl_df(df: pd.DataFrame) -> pd.DataFrame:
    dfc = df.copy()
    text_cols = OtpSegmentedPnlColumns.list_text_cols()
    percentage_cols = OtpSegmentedPnlColumns.list_percentage_cols()
    numeric_cols = OtpSegmentedPnlColumns.list_numeric_cols()

    dfc = standardize_cols_to_str(dfc, text_cols)
    dfc = standardize_cols_to_float(dfc, numeric_cols)
    dfc = standardize_from_pct_to_float(dfc, percentage_cols)

    return dfc


def enhance_otp_with_economic_activity(
    df: pd.DataFrame, rules: List[SegmentConfig]
) -> pd.DataFrame:
    dfc = df.copy()

    rules_dict: Dict[str, List[SegmentConfig]] = defaultdict(list)
    for rule in rules:
        rules_dict[rule.company_code].append(rule)

    activity: List[Activity | NAType] = []
    category: List[Category | NAType] = []

    for _, row in dfc.iterrows():
        company_code = row.get(OtpSegmentedPnlColumns.CompanyCode)
        assert isinstance(company_code, str)

        segment = row.get(OtpSegmentedPnlColumns.TPSegmentFinal)
        is_third_party = row.get(OtpSegmentedPnlColumns.InterCompanyFlag) == "E"

        for c in rules_dict.get(company_code.upper(), []):
            if not is_third_party and c.category == Category.OwnManufacturingThirdParty:
                continue

            if c.segment == segment:
                activity.append(c.activity)
                category.append(c.category)
                break
        else:
            activity.append(pd.NA)
            category.append(pd.NA)

    dfc[OtpSegmentedPnlColumns.Activity] = activity
    dfc[OtpSegmentedPnlColumns.Category] = category

    return dfc


class OtpSegmentedPnlETL(BaseETL):
    def __init__(self, fname: str | Path, sheet_name: str | None = None) -> None:
        super().__init__(fname, sheet_name)
        self.data: pd.DataFrame | None = None

    def load_data(self) -> pd.DataFrame:
        dfc = self.load(**{"header": None})
        dfc.columns = dfc.iloc[0]
        dfc = dfc.iloc[1:]
        dfc.reset_index(drop=True, inplace=True)

        self.data = dfc

        assert isinstance(self.data, pd.DataFrame)

        return self.data

    def transform(self, df: pd.DataFrame | None = None) -> pd.DataFrame:
        dfc = df if df is not None else self.load_data()
        return transform_otp_segmented_pnl_df(dfc)

    def transform_and_enhance(
        self, rules: List[SegmentConfig], df: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        dfc = self.transform(df)
        return enhance_otp_with_economic_activity(dfc, rules)
