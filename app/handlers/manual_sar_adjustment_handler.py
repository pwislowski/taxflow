from typing import List, Optional

import pandas as pd
import structlog

from app.enums import Category, OtpSegmentedPnlColumns

from .base import BaseHandler


class ManualEbitErosionAdjustmentHandler(BaseHandler):
    """Adjusts EBIT erosion via SAR allocations in specified categories."""

    def __init__(
        self,
        df: pd.DataFrame,
        df_otp: pd.DataFrame,
        target_ebit_erosion: float,
        column: Optional[OtpSegmentedPnlColumns] = OtpSegmentedPnlColumns.Total_SAR,
        isin: List[Category] = [Category.Distribution],
    ) -> None:
        """Initialize the ManualEbitErosionAdjustmentHandler.

        Args:
            df: pd.DataFrame - The DataFrame to be adjusted.
            df_otp: pd.DataFrame - The OTP DataFrame for EBIT margin calculation.
            target_ebit_erosion: float - The target EBIT erosion value (will be made negative).
            column: Optional[OtpSegmentedPnlColumns] - The SAR column to use (default: Total_SAR).
            isin: List[Category] - Categories to apply adjustments to (default: [DISTRIBUTION]).

        Raises:
            KeyError: If the DataFrame is missing required columns (Category, Total_SAR, column).
        """
        self.df_otp = df_otp
        super().__init__(df, column, isin)
        self.target_ebit_erosion = abs(target_ebit_erosion) * -1
        self.logger = structlog.get_logger(__name__)
        self.logger.info(
            "handler_initialized",
            handler=type(self).__name__,
            target_ebit_erosion=self.target_ebit_erosion,
        )

        required_df_cols = [
            OtpSegmentedPnlColumns.Category,
            OtpSegmentedPnlColumns.Total_SAR,
            column,
        ]
        missing_df_cols = [col for col in required_df_cols if col not in df.columns]
        if missing_df_cols:
            self.logger.error(
                "missing_required_columns",
                handler=type(self).__name__,
                missing_columns=missing_df_cols,
            )
            raise KeyError(
                f"Missing required columns in DataFrame for {self.__class__.__name__}: {missing_df_cols}"
            )

    def get_ebit_margin(self, df: pd.DataFrame) -> float:
        """Calculate the EBIT margin from the provided DataFrame.

        Args:
            df: pd.DataFrame - The DataFrame containing Net_Sales_Total and EBIT columns.

        Returns:
            float - The EBIT margin (EBIT sum / Net Sales sum).
        """
        self.logger.debug(
            "ebit_margin_calculation_started",
            handler=type(self).__name__,
            df_shape=df.shape,
        )
        net_sales = df[OtpSegmentedPnlColumns.Net_Sales_Total].sum()
        ebit = df[OtpSegmentedPnlColumns.Ebit].sum()
        self.logger.debug(
            "ebit_margin_inputs",
            handler=type(self).__name__,
            net_sales=net_sales,
            ebit=ebit,
        )
        margin = ebit / net_sales
        self.logger.debug(
            "ebit_margin_calculated",
            handler=type(self).__name__,
            margin=margin,
        )
        return margin

    def adjust_sar_ratio(self) -> None:
        """Adjust the SAR overhead allocations to achieve the target EBIT erosion.

        Calculates the required EBIT difference and proportionally adjusts the SAR columns
        (Sales and Marketing OH, OH Administration, Research and Technology OH) in the relevant categories.

        No return value; modifies self.df in place.
        """
        self.logger.info("sar_adjustment_started", handler=type(self).__name__)
        current_ebit_margin = self.get_ebit_margin(self.df_otp)
        self.logger.debug(
            "current_ebit_margin_loaded",
            handler=type(self).__name__,
            current_ebit_margin=current_ebit_margin,
        )
        target_ebit_margin = current_ebit_margin + self.target_ebit_erosion
        self.logger.debug(
            "target_ebit_margin_computed",
            handler=type(self).__name__,
            target_ebit_margin=target_ebit_margin,
        )
        target_ebit = self.get_net_sales_total() * target_ebit_margin
        self.logger.debug(
            "target_ebit_computed",
            handler=type(self).__name__,
            target_ebit=target_ebit,
        )
        current_ebit = self.df[OtpSegmentedPnlColumns.Ebit].sum()
        self.logger.debug(
            "current_ebit_loaded",
            handler=type(self).__name__,
            current_ebit=current_ebit,
        )
        ebit_diff = target_ebit - current_ebit
        self.logger.debug(
            "ebit_diff_computed",
            handler=type(self).__name__,
            ebit_diff=ebit_diff,
        )
        current_total_sar = self.df[OtpSegmentedPnlColumns.Total_SAR].sum()
        self.logger.debug(
            "current_total_sar_loaded",
            handler=type(self).__name__,
            current_total_sar=current_total_sar,
        )

        relevant_indexes = self.df[
            self.df[OtpSegmentedPnlColumns.Category].isin(self.isin)
        ].index
        self.logger.debug(
            "relevant_indexes_found",
            handler=type(self).__name__,
            count=len(relevant_indexes),
        )

        self.logger.info(
            "sar_adjustments_started",
            handler=type(self).__name__,
            rows=len(relevant_indexes),
        )
        for idx in relevant_indexes:
            temp_total_sar = self.df.loc[idx, OtpSegmentedPnlColumns.Total_SAR]
            columns = [
                OtpSegmentedPnlColumns.Sales_and_Marketing_OH,
                OtpSegmentedPnlColumns.OH_Administration,
                OtpSegmentedPnlColumns.Research_and_Technology_OH,
            ]
            for col in columns:
                self.df.loc[idx, col] = (
                    (temp_total_sar / current_total_sar)
                    * (self.df.loc[idx, col] / temp_total_sar)
                    * ebit_diff
                )

            self.df.loc[idx, OtpSegmentedPnlColumns.Total_SAR] = self.df.loc[
                idx, columns
            ].sum()
            self.logger.debug(
                "sar_row_adjusted",
                handler=type(self).__name__,
                index=idx,
                total_sar=self.df.loc[idx, OtpSegmentedPnlColumns.Total_SAR],
            )
        self.logger.info("sar_adjustment_completed", handler=type(self).__name__)

    def allocate(self) -> None:
        """Execute the SAR adjustment.

        Calls adjust_sar_ratio to perform the modifications.

        No return value; modifies self.df in place.
        """
        self.logger.debug("allocation_phase_started", handler=type(self).__name__)
        self.adjust_sar_ratio()
        self.logger.debug("allocation_phase_completed", handler=type(self).__name__)

    def process(self) -> pd.DataFrame:
        """Adjust SAR allocations for target EBIT erosion in ManualEbitErosionAdjustmentHandler.

        Performs the EBIT margin calculation using the OTP DataFrame and adjusts SAR overhead
        in specified categories to achieve the target erosion. Updates the DataFrame in place.

        Returns:
            pd.DataFrame: The DataFrame with adjusted SAR values affecting EBIT.

        Raises:
            KeyError: If required columns (e.g., Category, Total_SAR) are missing.
        """
        self.logger.info(
            "handler_process_started",
            handler=type(self).__name__,
            company_code=self.company_code,
        )
        try:
            self.allocate()
            return self.df
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
