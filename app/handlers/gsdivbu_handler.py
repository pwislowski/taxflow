from typing import List, Optional

import pandas as pd
import structlog

from app.enums import (
    Category,
    HighLevelSegmentedPnlColumns,
    OtpSegmentedPnlColumns,
)
from app.interfaces import GsEtlLoader

from .base import BaseHandler

ISIN = [Category.OwnManufacturingIC, Category.OwnManufacturingThirdParty]


class GsDivbuHandler(BaseHandler):
    """Handler for integrating GS divbu model and charges into the segmentation process.

    This class processes global shared services (GS) division/business unit (divbu) data,
    merging model information with charges allocation. It applies business unit mappings,
    calculates net expenses/incomes, and allocates them to appropriate P&L lines based
    on company code, profit centers, and economic activity segments.

    Attributes:
        df: The input DataFrame containing P&L data.
        company_code: The specific company code for processing.
        gs_model: The GsDivbuModelETL instance providing model data.
        gs_charges: The GsDivBuChargesETL instance providing charges data.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        company_code: str,
        gs_model: GsEtlLoader,
        gs_charges: GsEtlLoader,
        column: Optional[
            OtpSegmentedPnlColumns
        ] = OtpSegmentedPnlColumns.OH_Administration,
        isin: Optional[List[Category]] = ISIN,
        relevant_bus: List[str] = ["GS"],
    ) -> None:
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
            OtpSegmentedPnlColumns.OrgBU,
        ]
        missing_df_cols = [col for col in required_df_cols if col not in df.columns]
        if missing_df_cols:
            raise KeyError(
                f"Missing required columns in DataFrame for {self.__class__.__name__}: {missing_df_cols}"
            )

        self.data_gs_model = gs_model.generate_pipeline_context()
        self.data_gs_charges = gs_charges.generate_pipeline_context()
        self.relevant_bu = [x.upper() for x in relevant_bus]
        self.company_code = company_code
        super().__init__(df, column, isin)

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
        """Apply GS divbu integration and allocation to the DataFrame.

        Loads and merges GS model and charges data, performs lookups by company code
        and profit center, allocates internal/external costs and markups to activity/
        category columns, and updates net operating profit and other P&L totals.

        Returns:
            pd.DataFrame: The DataFrame with GS divbu charges integrated and segmented.

        Raises:
            KeyError: If required columns like Category, Net_Sales_Total, OrgBU, or OH_Administration are missing in GsDivbuHandler.
            ValueError: If allocation rules lead to invalid distributions (e.g., division by zero) in GsDivbuHandler.
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

    def _get_unallocated_line_items(self) -> pd.DataFrame:
        self.logger.debug(
            "unallocated_line_items_requested",
            handler=type(self).__name__,
            relevant_bu=self.relevant_bu,
        )
        df = self.df

        unallocated = df[
            (df[OtpSegmentedPnlColumns.Activity].isna())
            & (df[OtpSegmentedPnlColumns.OrgBU].isin(self.relevant_bu))
        ]  # type:ignore
        self.logger.debug(
            "unallocated_line_items_found",
            handler=type(self).__name__,
            count=len(unallocated),
        )
        return unallocated  # type:ignore

    def get_target_total(self) -> float:
        self.logger.debug(
            "target_total_calculation_started",
            handler=type(self).__name__,
            company_code=self.company_code,
        )
        column_total = self.df[
            self.df[OtpSegmentedPnlColumns.OrgBU].isin(self.relevant_bu)
        ][self.column].sum()
        self.logger.debug(
            "target_total_column_total_computed",
            handler=type(self).__name__,
            column_total=column_total,
        )

        this_year_charge = self.data_gs_model.get(self.company_code, {}).get(
            HighLevelSegmentedPnlColumns.TotalSAR, 0
        )
        self.logger.debug(
            "target_total_charge_loaded",
            handler=type(self).__name__,
            this_year_charge=this_year_charge,
        )

        target_total = column_total - this_year_charge
        self.logger.debug(
            "target_total_calculated",
            handler=type(self).__name__,
            target_total=target_total,
        )
        return target_total
