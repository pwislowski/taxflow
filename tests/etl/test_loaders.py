import importlib

import pandas as pd

from app.enums import (
    GsDivBuIncomeColumns,
    HighLevelSegmentedPnlColumns,
    OtpSegmentedPnlColumns,
    SapBwColumns,
)


def _load_module(module_path: str, **config_attrs):
    import app.config as config

    for name, value in config_attrs.items():
        if not hasattr(config, name):
            setattr(config, name, value)

    module = importlib.import_module(module_path)
    return importlib.reload(module)


def test_transform_grand_totals_df():
    from app.etl.loaders.grand_totals import transform_grand_totals_df

    df = pd.DataFrame(
        [
            [1, 2, 3, 10],
            [4, 5, 6, 20],
        ],
        columns=["company", "company_text", "mpr", "EBIT"],  # type:ignore
    )

    result = transform_grand_totals_df(df)

    assert list(result.columns) == [
        SapBwColumns.CompanyCode,
        SapBwColumns.CompanyCodeText,
        SapBwColumns.MprColumn,
        SapBwColumns.PnlItem,
        SapBwColumns.Amount,
    ]
    assert len(result) == 1
    assert result[SapBwColumns.CompanyCode].iloc[0] == "4"
    assert (
        result[SapBwColumns.PnlItem].iloc[0] == HighLevelSegmentedPnlColumns.TotalEBIT
    )


def test_transform_gs_divbu_charges_df():
    from app.etl.loaders.gs_divbu_charges import transform_gs_divbu_charges_df

    df = pd.DataFrame(
        [
            [
                "EBIT",
                "2024.01",
                "Jan",
                "AA01",
                "Company A",
                "PC1",
                "PC Text",
                "10001",
                "GL Text",
                "MPR",
                100,
            ],
            [
                "TOTAL NET SALES",
                "2024.01",
                "Jan",
                "AA01",
                "Company A",
                "PC1",
                "PC Text",
                "10001",
                "GL Text",
                "MPR",
                300,
            ],
        ]
    )

    result = transform_gs_divbu_charges_df(df, ["10001"])

    assert result.index.tolist() == ["AA01"]
    assert result[HighLevelSegmentedPnlColumns.TotalEBIT].iloc[0] == 100.0
    assert result[HighLevelSegmentedPnlColumns.TotalNetSales].iloc[0] == 300.0


def test_transform_gs_divbu_income_and_model_df():
    from app.etl.loaders.gs_divbu_model import (
        transform_gs_divbu_income_df,
        transform_gs_divbu_model_df,
    )

    df_income = pd.DataFrame(
        [
            [
                "AA01",
                1,
                2,
                3,
                4,
                5,
                2000,
                7,
                8,
                9,
                3000,
            ]
        ]
    )

    df_income_ok = transform_gs_divbu_income_df(df_income)

    assert df_income_ok[GsDivBuIncomeColumns.NetExpense].iloc[0] == 2000.0
    assert df_income_ok[GsDivBuIncomeColumns.NetIncome].iloc[0] == 3000.0

    df_gs_income = pd.DataFrame(
        {
            GsDivBuIncomeColumns.CompanyCode: ["AA01"],
            GsDivBuIncomeColumns.NetExpense: [2000.0],
            GsDivBuIncomeColumns.NetIncome: [3000.0],
        }
    )
    df_divbu_income = pd.DataFrame(
        {
            GsDivBuIncomeColumns.CompanyCode: ["AA01"],
            GsDivBuIncomeColumns.NetExpense: [1000.0],
            GsDivBuIncomeColumns.NetIncome: [500.0],
        }
    )

    result = transform_gs_divbu_model_df(df_gs_income, df_divbu_income)

    assert result[HighLevelSegmentedPnlColumns.TotalNetSales].iloc[0] == 3.5
    assert result[HighLevelSegmentedPnlColumns.TotalSAR].iloc[0] == -3.0
    assert result[HighLevelSegmentedPnlColumns.TotalEBIT].iloc[0] == 0.5


def test_transform_otp_segmented_pnl_df():
    otp_module = _load_module(
        "app.etl.loaders.otp_segmented_pnl",
        ActivitySegmentConfig=type("ActivitySegmentConfig", (), {}),
    )

    headers = OtpSegmentedPnlColumns.list_values()[:-2]
    text_cols = set(OtpSegmentedPnlColumns.list_text_cols())
    pct_cols = set(OtpSegmentedPnlColumns.list_percentage_cols())

    values = []
    for header in headers:
        if header == OtpSegmentedPnlColumns.CompanyCode:
            values.append("AA01")
        elif header in text_cols:
            values.append("text")
        elif header in pct_cols:
            values.append("50%")
        else:
            values.append("100")

    df = pd.DataFrame([values], columns=headers)  # type:ignore

    result = otp_module.transform_otp_segmented_pnl_df(df)

    assert result.shape == (1, 39)
    assert result[OtpSegmentedPnlColumns.CompanyCode].iloc[0] == "AA01"
    assert result[OtpSegmentedPnlColumns.LowerQuartile].iloc[0] == 0.5
    assert result[OtpSegmentedPnlColumns.Net_Sales_Total].iloc[0] == 100.0


def test_transform_royalties_df():
    from app.etl.loaders.royalties import transform_royalties_df

    df = pd.DataFrame(
        [
            [
                "EBIT",
                "AA01",
                "Company A",
                "PC1",
                "PC Text",
                "20002",
                "GL Text",
                "MPR",
                100,
            ],
            [
                "EBIT",
                "AA01",
                "Company A",
                "PC1",
                "PC Text",
                "20002",
                "GL Text",
                "MPR",
                150,
            ],
        ]
    )

    result = transform_royalties_df(df, accounts=["20002"])

    assert len(result) == 1
    assert result[SapBwColumns.Amount].iloc[0] == 250.0


def test_transform_rnd_fr09_df():
    rnd_module = _load_module(
        "app.etl.loaders.rnd_service", RndConfig=type("RndConfig", (), {})
    )

    df = pd.DataFrame(
        [
            ["AA01", "Company A", "EBIT", 1, "GL", "MPR", 100],
            ["AA01", "Company A", "EBIT", 2, "GL", "MPR", 50],
        ]
    )

    result = rnd_module.transform_rnd_fr09_df(df, accounts_in_scope=["1", "2"])

    assert len(result) == 1
    assert result[SapBwColumns.PnlItem].iloc[0] is None
    assert result[SapBwColumns.Amount].iloc[0] == 150.0


def test_transform_rnd_de03_df():
    rnd_module = _load_module(
        "app.etl.loaders.rnd_service", RndConfig=type("RndConfig", (), {})
    )

    df = pd.DataFrame(
        [
            [
                "DE03",
                "Company D",
                "TOTAL NET SALES",
                "CHF",
                "Swiss Franc",
                "7",
                "Profit Center 7",
                100,
                "GL",
                "MPR",
                1000,
            ],
            [
                "DE03",
                "Company D",
                "TOTAL SAR",
                "CHF",
                "Swiss Franc",
                "39",
                "Profit Center 39",
                200,
                "GL",
                "MPR",
                300,
            ],
        ]
    )

    result = rnd_module.transform_rnd_de03_df(
        df, company_code="DE03", accounts_in_scope=["100"]
    )

    total_net_sales = result[
        result[SapBwColumns.PnlItem] == HighLevelSegmentedPnlColumns.TotalNetSales
    ][SapBwColumns.Amount].iloc[0]
    total_sar = result[
        result[SapBwColumns.PnlItem] == HighLevelSegmentedPnlColumns.TotalSAR
    ][SapBwColumns.Amount].iloc[0]
    total_ebit = result[
        result[SapBwColumns.PnlItem] == HighLevelSegmentedPnlColumns.TotalEBIT
    ][SapBwColumns.Amount].iloc[0]

    assert total_net_sales == 1000.0
    assert total_sar == 300.0
    assert total_ebit == 1300.0
