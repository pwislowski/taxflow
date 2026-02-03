from typing import Optional

import pandas as pd
import structlog

from app.enums import OtpSegmentedPnlColumns

from .base import BaseHandler


class OtherIncomeHandler(BaseHandler):
    def __init__(
        self,
        df: pd.DataFrame,
        column: Optional[OtpSegmentedPnlColumns] = OtpSegmentedPnlColumns.Other_Income,
    ) -> None:
        super().__init__(df, column, self.get_isin_categories(df))

        self.logger = structlog.get_logger(__name__)
        self.logger.info("handler_initialized", handler=type(self).__name__)

        required_df_cols = [
            OtpSegmentedPnlColumns.Category,
            OtpSegmentedPnlColumns.Net_Sales_Total,
            column,
        ]
        missing_df_cols = [col for col in required_df_cols if col not in df.columns]
        if missing_df_cols:
            raise KeyError(
                f"Missing required columns in DataFrame for {self.__class__.__name__}: {missing_df_cols}"
            )

    def allocate_to_line_items_by_net_sales(self) -> None:
        self.logger.debug(
            "allocation_started",
            handler=type(self).__name__,
            base="net_sales",
        )
        relevant_indexes = self.df[
            self.df[OtpSegmentedPnlColumns.Category].isin(self.isin)
        ].index

        self.logger.debug(
            "relevant_indexes_found",
            handler=type(self).__name__,
            count=len(relevant_indexes),
        )

        base_total = self.get_net_sales_total()
        target_total = self.get_target_total()

        self.logger.debug(
            "allocation_totals_computed",
            handler=type(self).__name__,
            base_total=base_total,
            target_total=target_total,
        )

        if base_total == 0:
            self.logger.debug(
                "allocation_skipped",
                handler=type(self).__name__,
                reason="base_zero",
            )
            return

        self._allocate_proportionally(
            base_column=OtpSegmentedPnlColumns.Net_Sales_Total,
            base_total=base_total,
            target_total=target_total,
            relevant_indexes=relevant_indexes,
        )

        self.logger.debug(
            "allocation_completed",
            handler=type(self).__name__,
            base="net_sales",
        )

    def allocate(self) -> None:
        self.logger.debug("allocation_phase_started", handler=type(self).__name__)
        self.allocate_to_line_items_by_net_sales()
        self.logger.debug("allocation_phase_completed", handler=type(self).__name__)

    def process(self) -> pd.DataFrame:
        """Apply other income allocation to the DataFrame.

        Identifies relevant rows based on P&L item types (e.g., other income),
        maps profit centers to segments using configuration rules, and allocates
        values to activity/category columns. Updates totals if necessary.

        Returns:
            pd.DataFrame: The segmented DataFrame with allocated other income.

        Raises:
            KeyError: If required columns like Category, Net_Sales_Total, or Other_Income are missing in OtherIncomeHandler.
            ValueError: If allocation rules lead to invalid distributions (e.g., division by zero) in OtherIncomeHandler.
        """
        self.logger.info(
            "handler_process_started",
            handler=type(self).__name__,
            company_code=self.company_code,
        )
        try:
            return super().process()
        except Exception as e:
            self.logger.error(
                "handler_process_failed",
                handler=type(self).__name__,
                company_code=self.company_code,
                error=str(e),
            )
            raise
        finally:
            self.logger.info(
                "handler_process_completed",
                handler=type(self).__name__,
                company_code=self.company_code,
            )
