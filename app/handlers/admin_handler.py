from typing import Callable, List, Optional

import pandas as pd
import structlog

from app.enums import Category, OtpSegmentedPnlColumns, SapBwColumns

from .base import BaseHandler

ISIN = [
    Category.OwnManufacturingIC,
    Category.OwnManufacturingThirdParty,
    Category.Distribution,
    Category.ContractManufacturing,
]


class AdminHandler(BaseHandler):
    """Handler for allocating administrative overhead costs in the P&L segmentation.

    This class identifies administrative overhead items in the DataFrame,
    applies allocation rules based on profit centers or other keys, and distributes
    the costs to appropriate activity and category columns (e.g., routine/entrepreneur).

    Attributes:
        df: The input DataFrame containing P&L data.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        company_code: Optional[str] = None,
        df_royalty: Optional[pd.DataFrame] = None,
        column: Optional[
            OtpSegmentedPnlColumns
        ] = OtpSegmentedPnlColumns.OH_Administration,
        isin: Optional[List[Category]] = ISIN,
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

        enhance = False
        if company_code is not None or df_royalty is not None:
            assert company_code is not None and df_royalty is not None, (
                "company_code and df_royalty must be provided"
            )

            required_royalty_cols = [
                SapBwColumns.CompanyCode,
                SapBwColumns.Amount,
            ]
            missing_royalty_cols = [
                col for col in required_royalty_cols if col not in df_royalty.columns
            ]
            if missing_royalty_cols:
                raise KeyError(
                    f"Missing required columns in df_royalty for {self.__class__.__name__}: {missing_royalty_cols}"
                )
            enhance = True

        self.df_royalty: pd.DataFrame = df_royalty  # type: ignore
        self.company_code: str = company_code  # type: ignore

        self.royalty_expense = self._get_royalty_expense() if enhance else 0.0

    def _get_royalty_expense(self) -> float:
        self.logger.debug(
            "royalty_expense_calculation_started",
            handler=type(self).__name__,
            company_code=self.company_code,
        )
        df = self.df_royalty
        df = df[
            (df[SapBwColumns.CompanyCode] == self.company_code)
            & (df[SapBwColumns.Amount] < 0)
        ]

        if df.empty:
            result = 0.0
        else:
            result = df[SapBwColumns.Amount].iloc[0]  # type:ignore

        self.logger.debug(
            "royalty_expense_calculated",
            handler=type(self).__name__,
            royalty_expense=result,
        )
        return result

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
        target_total -= self.royalty_expense

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
        """Apply administrative overhead allocation to the DataFrame.

        Scans for administrative OH rows, maps them to segments using predefined rules,
        and allocates values proportionally (e.g., based on net sales or headcount proxies).
        Updates the DataFrame with enhanced activity/category assignments.

        Returns:
            pd.DataFrame: The DataFrame with administrative costs segmented.

        Raises:
            KeyError: If required columns like Category, Net_Sales_Total, or OH_Administration are missing in AdminHandler.
            ValueError: If allocation rules lead to invalid distributions (e.g., division by zero) in AdminHandler.
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
