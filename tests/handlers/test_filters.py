import pandas as pd

from app.enums import Category, OtpSegmentedPnlColumns
from app.handlers.filters import create_business_unit_category_filter


def _sample_df():
    return pd.DataFrame(
        {
            OtpSegmentedPnlColumns.OrgBU: ["BU1", "BU2", "BU3", "BU1"],
            OtpSegmentedPnlColumns.Category: [
                Category.Distribution,
                Category.ContractManufacturing,
                Category.OwnManufacturingIC,
                Category.Distribution,
            ],
        }
    )


def test_filter_with_category_applies_bu_and_category():
    df = _sample_df()
    filter_func = create_business_unit_category_filter(
        business_units=["BU1", "BU3"],
        category=Category.Distribution,
    )

    mask = filter_func(df)

    assert mask.tolist() == [True, False, False, True]


def test_filter_with_default_categories_ignores_bu_list():
    df = _sample_df()
    filter_func = create_business_unit_category_filter(
        business_units=["BU1"],
        default_categories=[
            Category.ContractManufacturing,
            Category.OwnManufacturingIC,
        ],
    )

    mask = filter_func(df)

    assert mask.tolist() == [False, True, True, False]


def test_filter_with_only_business_units():
    df = _sample_df()
    filter_func = create_business_unit_category_filter(business_units=["BU2", "BU3"])

    mask = filter_func(df)

    assert mask.tolist() == [False, True, True, False]
