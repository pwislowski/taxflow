from typing import Callable, List, Optional

import pandas as pd
from structlog import contextvars

from app.enums import Activity, Category, OtpSegmentedPnlColumns
from app.interfaces import SegmentationHandler


class BaseHandler(SegmentationHandler):
    """Base class for segmentation handlers in the OTP pipeline.

    This abstract base class provides common functionality for allocating and deallocating
    values in P&L line items based on activity and category segmentation. It handles
    unallocated items, computes totals for net sales and COGS, and provides a process
    method to execute allocation logic.

    Subclasses must implement the `allocate` method to define specific business rules
    for distributing values across activities (routine/entrepreneur) and categories.

    Attributes:
        df: The input DataFrame being processed, containing segmented P&L data.
        column: Optional specific column (OtpSegmentedPnlColumns) targeted for allocation.
        isin: List of Category enums used to filter relevant rows for totals calculation.
        filter_func: Optional callable for custom row filtering logic.
        df_unallocated: Subset of df containing rows where Activity is NaN (unallocated items).
    """

    def __init__(
        self,
        df: pd.DataFrame,
        column: Optional[OtpSegmentedPnlColumns] = None,
        isin: Optional[List[Category]] = None,
        filter_func: Optional[Callable[[pd.DataFrame], pd.Series]] = None,
        company_code: Optional[str] = None,
    ) -> None:
        """
        Initialize the BaseHandler instance with the DataFrame and configuration parameters.

        Sets up the DataFrame reference, target column, category filter, and extracts
        unallocated line items.

        Args:
            df: The pandas DataFrame with P&L data, expected to have activity and category columns.
            column: Optional column name (from OtpSegmentedPnlColumns) for allocation target.
            isin: Optional list of Category values to filter rows for total calculations.
            filter_func: Optional callable that takes a DataFrame and returns a boolean Series
                        to filter rows for allocation. If provided, this overrides the isin filter.
        """
        self.df = df
        self.column = column
        self.isin = [] if not isin else isin
        self.filter_func = filter_func
        self.company_code = company_code
        if (
            self.company_code is None
            and OtpSegmentedPnlColumns.CompanyCode in df.columns
        ):
            uniqs = df[OtpSegmentedPnlColumns.CompanyCode].dropna().unique()
            if len(uniqs) == 1:
                self.company_code = str(uniqs[0])
        if self.company_code is not None:
            contextvars.bind_contextvars(company_code=self.company_code)
        self.df_unallocated = self._get_unallocated_line_items()

    def _get_unallocated_line_items(self) -> pd.DataFrame:
        """Extract rows from the DataFrame where Activity is NaN, representing unallocated items.

        Returns:
            A filtered DataFrame containing only unallocated line items.
        """
        df = self.df
        is_na = df[OtpSegmentedPnlColumns.Activity].isna()
        oth = self._get_relevant_rows_mask()
        mask = is_na & oth

        return df[mask]  # type:ignore

    def _get_relevant_rows_mask(self) -> pd.Series:
        """Get boolean mask for rows relevant to allocation based on filter_func or isin.

        Returns:
            A boolean Series indicating which rows should be included in allocation.
        """
        if self.filter_func is not None:
            return self.filter_func(self.df)
        if self.isin:
            return self.df[OtpSegmentedPnlColumns.Category].isin(self.isin)  # type:ignore
        return pd.Series([True] * len(self.df), index=self.df.index)  # type:ignore

    def get_net_sales_total(self) -> float:
        """Compute the total net sales value for rows matching the filter (filter_func or isin).

        Filters the DataFrame by the configured filter and sums the 'Net Sales Total' column.

        Returns:
            The sum of net sales as a float.
        """
        df = self.df
        mask = self._get_relevant_rows_mask()

        return df[mask][OtpSegmentedPnlColumns.Net_Sales_Total].sum()

    def get_cogs_total(self) -> float:
        """Compute the total COGS (Cost of Goods Sold) for rows matching the filter (filter_func or isin).

        Filters the DataFrame by the configured filter and sums the 'COGS' column.

        Returns:
            The sum of COGS as a float.
        """
        df = self.df
        mask = self._get_relevant_rows_mask()

        return df[mask][OtpSegmentedPnlColumns.Cogs].sum()

    def _allocate_proportionally(
        self,
        *,
        base_column: OtpSegmentedPnlColumns,
        base_total: float,
        target_total: float,
        relevant_indexes: pd.Index,
    ) -> None:
        """Allocate target_total across rows proportionally to base_column values.

        Args:
            base_column: Column used to compute allocation proportions.
            target_total: Total amount to distribute.
            relevant_indexes: Row indexes to allocate across.

        Raises:
            ValueError: If the base total is zero.
        """

        for i in relevant_indexes:
            base_value = self.df.loc[i, base_column]
            allocated = target_total * base_value / base_total
            self.df.loc[i, self.column] += allocated

    def allocate(self) -> None:
        """Abstract method to implement allocation logic for unallocated items.

        Subclasses must override this to distribute values from `self.column` across
        activity and category segments based on business rules (e.g., proportional to net sales).

        This method modifies `self.df` in place by filling NaN values in the Activity column
        and updating the target column.
        """
        ...

    def deallocate(self) -> None:
        """Reset allocated values in unallocated rows back to zero in the target column.

        Iterates over indices of unallocated items and sets the value in `self.column` to 0.
        Modifies `self.df` in place.
        """
        for index in self.df_unallocated.index:
            self.df.loc[index, self.column] = 0

    def get_target_total(self) -> float:
        """Calculate the total value to be allocated from the target column in unallocated rows.

        Sums the values in `self.column` from `self.df_unallocated`.

        Returns:
            The total target value as a float.
        """
        df = self.df_unallocated
        return df[self.column].sum()

    def process(self) -> pd.DataFrame:
        """Execute the full allocation and deallocation process.

        Calls `allocate` to distribute values, then `deallocate` to reset unallocated
        items if needed, and returns the modified DataFrame.

        Returns:
            The processed DataFrame with allocations applied.

        Note:
            `deallocate` is called after `allocate`, which may seem counterintuitive.
            In practice, `allocate` should handle filling all items, and `deallocate`
            ensures any residuals are zeroed if allocation doesn't cover everything.
        """
        self.allocate()
        self.deallocate()
        return self.df

    @staticmethod
    def get_isin_categories(df: pd.DataFrame) -> List[Category]:
        """Determine the relevant Category list based on existing Activity values in the DataFrame.

        Analyzes unique non-NaN Activity values to decide between routine (distribution/contract)
        or entrepreneurial (own manufacturing) categories.

        Args:
            df: The DataFrame to inspect for Activity values.

        Returns:
            A list of appropriate Category enums for filtering (e.g., for total calculations).
        """
        uniq = df[OtpSegmentedPnlColumns.Activity].unique().tolist()
        uniq = [x for x in uniq if not pd.isna(x)]  # filter out pd.NAs for comparison
        isin = [Category.Distribution, Category.ContractManufacturing]

        if Activity.Entrepreneur in uniq:
            isin = [
                Category.OwnManufacturingIC,
                Category.OwnManufacturingThirdParty,
            ]

        return isin
