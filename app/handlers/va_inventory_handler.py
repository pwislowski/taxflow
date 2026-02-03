from typing import List, Optional

import pandas as pd
import structlog

from app.enums import Category, OtpSegmentedPnlColumns

from .base import BaseHandler


class VaInventoryReceivablesHandler(BaseHandler):
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
        column: Optional[OtpSegmentedPnlColumns] = OtpSegmentedPnlColumns.Total_SAR,
        isin: Optional[List[Category]] = None,
    ) -> None:
        super().__init__(df, column, isin if isin else [])

        self.logger = structlog.get_logger(__name__)
        self.logger.info("handler_initialized", handler=type(self).__name__)

        required_df_cols = [
            OtpSegmentedPnlColumns.Gross_Profit_After_Variances,
            OtpSegmentedPnlColumns.VA_Inventory_Receivables_non_c,
        ]
        missing_df_cols = [col for col in required_df_cols if col not in df.columns]
        if missing_df_cols:
            raise KeyError(
                f"Missing required columns in DataFrame for {self.__class__.__name__}: {missing_df_cols}"
            )

    def allocate(self) -> None:
        self.logger.debug("allocation_phase_started", handler=type(self).__name__)
        self.df[OtpSegmentedPnlColumns.Gross_Profit_After_Variances] += self.df[
            OtpSegmentedPnlColumns.VA_Inventory_Receivables_non_c
        ]
        self.logger.debug(
            "va_inventory_adjustment_applied",
            handler=type(self).__name__,
        )
        self.logger.debug("allocation_phase_completed", handler=type(self).__name__)

    def process(self) -> pd.DataFrame:
        """Apply VA inventory and receivables adjustments to the DataFrame.

        Updates Gross_Profit_After_Variances by adding VA_Inventory_Receivables_non_c
        values. Ensures P&L totals reflect these adjustments for consistency.

        Returns:
            pd.DataFrame: The DataFrame with VA inventory adjustments applied.

        Raises:
            KeyError: If required columns like Gross_Profit_After_Variances or VA_Inventory_Receivables_non_c are missing in VaInventoryReceivablesHandler.
            ValueError: If adjustments lead to inconsistencies in VaInventoryReceivablesHandler.
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
