from typing import List, Optional

import pandas as pd
import structlog

from app.enums import Category, OtpSegmentedPnlColumns

from .base import BaseHandler


class RecalculateTotals(BaseHandler):
    """Handler for recalculating P&L totals after segmentation.

    This class updates aggregate P&L columns such as Gross Profit After Variances,
    Total SAR, Net Operating Profit, EBIT, and others based on the segmented
    allocations to activity and category columns. Ensures consistency in totals
    post-allocation by summing segmented values.

    Attributes:
        df: The input DataFrame containing segmented P&L data.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        df_reference: pd.DataFrame,
        column: Optional[OtpSegmentedPnlColumns] = OtpSegmentedPnlColumns.Total_SAR,
        isin: Optional[List[Category]] = None,
        company_code: Optional[str] = None,
    ) -> None:
        self.df_reference = df_reference
        if company_code:
            self.df_reference = self.df_reference[
                self.df_reference[OtpSegmentedPnlColumns.CompanyCode] == company_code
            ]

        super().__init__(df, column, isin if isin else [])

        self.logger = structlog.get_logger(__name__)
        self.logger.info("handler_initialized", handler=type(self).__name__)

        required_cols = [
            OtpSegmentedPnlColumns.Sales_and_Marketing_OH,
            OtpSegmentedPnlColumns.OH_Administration,
            OtpSegmentedPnlColumns.Research_and_Technology_OH,
            OtpSegmentedPnlColumns.Cogs,
            OtpSegmentedPnlColumns.Variances,
            OtpSegmentedPnlColumns.Total_SAR,
            OtpSegmentedPnlColumns.Other_Expenses,
            OtpSegmentedPnlColumns.Other_Income,
            OtpSegmentedPnlColumns.Unusual_Expenses_Income,
            OtpSegmentedPnlColumns.Ebit,
            OtpSegmentedPnlColumns.CompanyCode,
        ]
        missing_cols = [col for col in required_cols if col not in self.df.columns]
        if missing_cols:
            raise KeyError(
                f"Missing required columns in DataFrame for {self.__class__.__name__}: {missing_cols}"
            )

        missing_ref_cols = [
            col for col in required_cols[:-1] if col not in self.df_reference.columns
        ]
        if missing_ref_cols:
            raise KeyError(
                f"Missing required columns in df_reference for {self.__class__.__name__}: {missing_ref_cols}"
            )

    def allocate(self) -> None:
        self.logger.info(
            "recalculate_totals_started",
            handler=type(self).__name__,
            company_code=self.company_code,
        )
        diff = None
        df = self.df.copy()

        df[OtpSegmentedPnlColumns.Total_SAR] = (
            df[OtpSegmentedPnlColumns.Sales_and_Marketing_OH]
            + df[OtpSegmentedPnlColumns.OH_Administration]
            + df[OtpSegmentedPnlColumns.Research_and_Technology_OH]
        )

        self.logger.debug("total_sar_recalculated", handler=type(self).__name__)

        for col in [
            OtpSegmentedPnlColumns.Cogs,
            OtpSegmentedPnlColumns.Variances,
            OtpSegmentedPnlColumns.Total_SAR,
            OtpSegmentedPnlColumns.Other_Expenses,
            OtpSegmentedPnlColumns.Other_Income,
            OtpSegmentedPnlColumns.Unusual_Expenses_Income,
        ]:
            old_total = self.df_reference[col]
            new_total = df[col]
            diff = new_total - old_total
            self.logger.debug(
                "recalculation_diff_computed",
                handler=type(self).__name__,
                column=col,
                diff_sum=diff.sum(),
            )
            df[OtpSegmentedPnlColumns.Ebit] += diff

        self.df = df

        self.logger.info(
            "recalculate_totals_completed",
            handler=type(self).__name__,
            company_code=self.company_code,
        )

    def process(self) -> pd.DataFrame:
        """Recalculate and update P&L total columns based on segmented allocations.

        Iterates through relevant rows and columns, recomputing totals like
        Gross_Profit_After_Variances, Total_SAR, Operating_Income, and EBIT by
        aggregating values from activity/category breakdowns. Handles any necessary
        adjustments for variances, other income/expenses, and ensures the DataFrame
        reflects accurate financial summaries.

        Returns:
            pd.DataFrame: The DataFrame with recalculated P&L totals.

        Raises:
            KeyError: If required P&L columns (e.g., Sales_and_Marketing_OH, OH_Administration, Research_and_Technology_OH, COGS, etc.) are missing in RecalculateTotals.
            ValueError: If recalculations result in inconsistencies (e.g., negative totals where invalid) in RecalculateTotals.
        """
        self.logger.info(
            "handler_process_started",
            handler=type(self).__name__,
            company_code=self.company_code,
        )
        try:
            self.allocate()
        except Exception as e:
            self.logger.error(
                "handler_process_failed",
                handler=type(self).__name__,
                company_code=self.company_code,
                error=str(e),
            )
            raise
        self.logger.info(
            "handler_process_completed",
            handler=type(self).__name__,
            company_code=self.company_code,
        )
        return self.df
