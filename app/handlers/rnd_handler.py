from typing import TYPE_CHECKING, Callable, List, Optional

import pandas as pd
import structlog

from app.enums import (
    Category,
    HighLevelSegmentedPnlColumns,
    OtpSegmentedPnlColumns,
    SapBwColumns,
)

from .base import BaseHandler

ISIN = [Category.OwnManufacturingIC, Category.OwnManufacturingIC]


class RndHandler(BaseHandler):
    """Handler for allocating R&D overhead costs in the P&L segmentation.

    This class identifies research and technology overhead items in the DataFrame,
    applies allocation rules based on profit centers or activity segments, and distributes
    the costs to entrepreneur/routine categories, often weighted by gross profit or other metrics.

    Attributes:
        df: The input DataFrame containing P&L data.

    Raises:
        KeyError: If required columns like Category, Net_Sales_Total, or Research_and_Technology_OH are missing in RndHandler.
        ValueError: If allocation rules lead to invalid distributions (e.g., division by zero) in RndHandler.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        company_code: str,
        column: Optional[
            OtpSegmentedPnlColumns
        ] = OtpSegmentedPnlColumns.Research_and_Technology_OH,
        isin: Optional[List[Category]] = ISIN,
        df_rnd_services: Optional[pd.DataFrame] = None,
        filter_func: Optional[Callable[[pd.DataFrame], pd.Series]] = None,
    ) -> None:
        super().__init__(df, column, isin, filter_func)

        self.logger = structlog.get_logger(__name__)
        self.logger.info(
            "handler_initialized",
            handler=type(self).__name__,
            company_code=company_code,
        )

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

        self.company_code = company_code
        self.df_rnd_services = df_rnd_services

    def get_rnd_services_costs(self) -> float:
        self.logger.debug(
            "rnd_services_costs_requested",
            handler=type(self).__name__,
            company_code=self.company_code,
        )
        if self.df_rnd_services is None:
            self.logger.debug(
                "rnd_services_costs_skipped",
                handler=type(self).__name__,
                company_code=self.company_code,
                reason="missing_df_rnd_services",
            )
            return 0.0

        df = self.df_rnd_services
        sar_cost = df[
            (df[SapBwColumns.CompanyCode] == self.company_code)
            & (df[SapBwColumns.PnlItem] == HighLevelSegmentedPnlColumns.TotalSAR)
        ][SapBwColumns.Amount]

        if sar_cost.empty:  # type:ignore
            self.logger.debug(
                "rnd_services_costs_skipped",
                handler=type(self).__name__,
                company_code=self.company_code,
                reason="empty_sar_cost",
            )
            return 0.0

        sar_cost = sar_cost.iloc[0]  # type:ignore

        if TYPE_CHECKING:
            assert isinstance(sar_cost, float)

        self.logger.debug(
            "rnd_services_costs_calculated",
            handler=type(self).__name__,
            company_code=self.company_code,
            sar_cost=sar_cost,
        )

        return sar_cost

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
        target_total -= self.get_rnd_services_costs()

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
