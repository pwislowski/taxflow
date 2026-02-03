"""Filter functions for handler row selection.

This module provides reusable filter functions that can be passed to handlers
to customize which rows are included in allocation calculations.

Filter functions take a DataFrame and return a boolean Series (mask) indicating
which rows should be included.
"""

from typing import Callable, List, Optional

import pandas as pd

from app.enums import Category, OtpSegmentedPnlColumns


def create_business_unit_category_filter(
    business_units: List[str],
    category: Optional[Category] = None,
    default_categories: Optional[List[Category]] = None,
) -> Callable[[pd.DataFrame], pd.Series]:
    """Create a filter that routes specific business units to a particular category.

    If category and default_categories are provided, for specified business units,
    only rows with the target category are selected.

    Args:
        business_units: List of business unit codes (e.g., ['A', 'B']).
        category: The category to use for the specified business units (optional).

    Returns:
        A filter function that applies conditional category filtering based on business unit.

    Example:
        >>> # With category filtering
        >>> filter_func = create_business_unit_category_filter(
        ...     business_units=[
        ...         "A",
        ...         "B",
        ...     ],
        ...     category=Category.SOME_CATEGORY,
        ... )
        >>> handler = SalesMarketingHandler(
        ...     df,
        ...     filter_func=filter_func,
        ...     isin=[
        ...         Category.SOME_CATEGORY
        ...     ],
        ... )
    """

    def filter_func(df: pd.DataFrame) -> pd.Series:
        # Check which rows belong to the specified business units
        is_special_bu = df[OtpSegmentedPnlColumns.OrgBU].isin(business_units)

        if category is not None:
            is_category = df[OtpSegmentedPnlColumns.Category] == category
            return is_special_bu & is_category  # type:ignore

        if default_categories:
            return df[OtpSegmentedPnlColumns.Category].isin(default_categories)  # type:ignore

        return is_special_bu  # type:ignore

    return filter_func
