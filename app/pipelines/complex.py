from typing import TYPE_CHECKING, Dict, List, Tuple, Type

import pandas as pd
import structlog

from app.enums import Category
from app.handlers import (
    AdminHandler,
    CogsHandler,
    GsDivbuHandler,
    OtherExpenseHandler,
    OtherIncomeHandler,
    RecalculateTotals,
    RndHandler,
    SalesMarketingHandler,
    VaInventoryReceivablesHandler,
    VarianceHandler,
    filters,
)
from app.handlers.royalty_handler import RoyaltyHandler
from app.handlers.unusual_items_handler import UnusualItemsHandler
from app.interfaces import SegmentationHandler

from .base import BasePipeline
from .context import PipelineContext


class EntrepreneurWithRoutinePipeline(BasePipeline):
    """Pipeline implementation for entrepreneur and routine segmentation

    This class chains multiple segmentation handlers in a fixed sequence to process the input DataFrame.
    It enhances the DataFrame with activity and category columns, applies business-specific logic,
    and recalculates P&L totals.

    Attributes:
        df: The initial input DataFrame (otp_pnl filtered by company).
        df_ok: The DataFrame after progressive processing by each handler.
        context: The PipelineContext providing shared data loaders and company code.
    """

    def __init__(self, df: pd.DataFrame, context: PipelineContext) -> None:
        self.context = context
        if TYPE_CHECKING:
            assert isinstance(context.company_code, str)

        super().__init__(df, context)
        self.logger = structlog.get_logger(f"{__name__}.{type(self).__name__}")
        self.logger.info(
            "pipeline_initialized",
            pipeline=type(self).__name__,
            company_code=context.company_code,
        )

    def process(self) -> pd.DataFrame:
        """Process the input DataFrame by sequentially applying segmentation handlers.

        Handlers are executed in order: CogsHandler, VaInventoryReceivablesHandler,
        VarianceHandler, OtherExpenseHandler, OtherIncomeHandler, UnusualItemsHandler,
        GsDivbuHandler, RoyaltyHandler, SalesMarketingHandler, RndHandler, AdminHandler,
        RecalculateTotals. Each handler receives the output of the previous one and modifies
        the DataFrame (adding/enhancing columns as needed).

        Returns:
            pd.DataFrame: The fully processed DataFrame with segmentation applied.

        Raises:
            Any exceptions raised by individual handler instantiation or process() calls,
            such as missing data, invalid configurations, or transformation errors.
        """
        self.logger.info(
            "pipeline_processing_started",
            pipeline=type(self).__name__,
            company_code=self.context.company_code,
        )
        # Store handler configurations: (class, extra_args, extra_kwargs)
        handler_configs: List[Tuple[Type[SegmentationHandler], List, Dict]] = [
            (CogsHandler, [], {}),
            (VaInventoryReceivablesHandler, [], {}),
            (VarianceHandler, [], {}),
            (OtherExpenseHandler, [], {}),
            (OtherIncomeHandler, [], {}),
            (UnusualItemsHandler, [], {}),
            (SalesMarketingHandler, [], {}),
            (
                RndHandler,
                [self.context.company_code],
                {"df_rnd_services": self.context.rnd_services},
            ),
            (
                GsDivbuHandler,
                [
                    self.context.company_code,
                    self.context.gsdivbu_model,
                    self.context.gsdivbu_charges,
                ],
                {},
            ),
            (
                RoyaltyHandler,
                [self.context.company_code, self.context.royalties],
                {},
            ),
            (
                AdminHandler,
                [],
                {
                    "company_code": self.context.company_code,
                    "df_royalty": self.context.royalties,
                },
            ),
            (
                RecalculateTotals,
                [self.context.otp_pnl],
                {"company_code": self.context.company_code},
            ),
        ]

        for i, (handler_cls, extra_args, extra_kwargs) in enumerate(handler_configs):
            # First argument is always the dataframe
            df_to_use = self.df if i == 0 else self.df_ok

            # Instantiate with df + any extra parameters
            handler = handler_cls(df_to_use, *extra_args, **extra_kwargs)
            self.logger.debug(
                "handler_processing_started",
                pipeline=type(self).__name__,
                handler=handler_cls.__name__,
            )
            processed_df = handler.process()
            self.df_ok = processed_df
            self.logger.debug(
                "handler_processing_completed",
                pipeline=type(self).__name__,
                handler=handler_cls.__name__,
            )

        self.logger.info(
            "pipeline_processing_completed",
            pipeline=type(self).__name__,
            final_shape=self.df_ok.shape,
        )
        return self.df_ok


class EntrepreneurWithRoutineAndExternalCosts(BasePipeline):
    """Pipeline implementation for entrepreneur and routine segmentation

    This class chains multiple segmentation handlers in a fixed sequence to process the input DataFrame.
    It enhances the DataFrame with activity and category columns, applies business-specific logic,
    and recalculates P&L totals.

    Attributes:
        df: The initial input DataFrame (otp_pnl filtered by company).
        df_ok: The DataFrame after progressive processing by each handler.
        context: The PipelineContext providing shared data loaders and company code.
    """

    def __init__(self, df: pd.DataFrame, context: PipelineContext) -> None:
        self.context = context
        if TYPE_CHECKING:
            assert isinstance(context.company_code, str)

        super().__init__(df, context)
        self.logger = structlog.get_logger(f"{__name__}.{type(self).__name__}")
        self.logger.info(
            "pipeline_initialized",
            pipeline=type(self).__name__,
            company_code=context.company_code,
        )

    def process(self) -> pd.DataFrame:
        """Process the input DataFrame by sequentially applying segmentation handlers.

        Handlers are executed in order: CogsHandler, VaInventoryReceivablesHandler,
        VarianceHandler, OtherExpenseHandler, OtherIncomeHandler, UnusualItemsHandler,
        GsDivbuHandler, RoyaltyHandler, SalesMarketingHandler, RndHandler, AdminHandler,
        SalesMarketingHandler, RndHandler, AdminHandler, RecalculateTotals. Each handler
        receives the output of the previous one and modifies the DataFrame (adding/enhancing columns as needed).

        Returns:
            pd.DataFrame: The fully processed DataFrame with segmentation applied.

        Raises:
            Any exceptions raised by individual handler instantiation or process() calls,
            such as missing data, invalid configurations, or transformation errors.
        """
        self.logger.info(
            "pipeline_processing_started",
            pipeline=type(self).__name__,
            company_code=self.context.company_code,
        )
        # Create filter for business units to route to OWN_MANUFACTURING_THIRD_PARTY

        business_units = self.context.external_cost_business_units
        special_kwargs = {
            "filter_func": pd.Series,
            "isin": [Category.OwnManufacturingThirdParty],
        }

        if business_units:
            special_kwargs["filter_func"] = (
                filters.create_business_unit_category_filter(
                    business_units=list(business_units) if business_units else []
                )
            )

        # Store handler configurations: (class, extra_args, extra_kwargs)
        handler_configs: List[Tuple[Type[SegmentationHandler], List, Dict]] = [
            (CogsHandler, [], {}),
            (VaInventoryReceivablesHandler, [], {}),
            (VarianceHandler, [], {}),
            (OtherExpenseHandler, [], {}),
            (OtherIncomeHandler, [], {}),
            (UnusualItemsHandler, [], {}),
            (
                GsDivbuHandler,
                [
                    self.context.company_code,
                    self.context.gsdivbu_model,
                    self.context.gsdivbu_charges,
                ],
                {},
            ),
            (
                RoyaltyHandler,
                [self.context.company_code, self.context.royalties],
                {},
            ),
            # process special cases
            (
                SalesMarketingHandler,
                [],
                {**special_kwargs},
            ),
            (
                RndHandler,
                [self.context.company_code],
                {
                    "df_rnd_services": self.context.rnd_services,
                    **special_kwargs,
                },
            ),
            (
                AdminHandler,
                [],
                {
                    "company_code": self.context.company_code,
                    "df_royalty": self.context.royalties,
                    **special_kwargs,
                },
            ),
            (SalesMarketingHandler, [], {}),
            (
                RndHandler,
                [self.context.company_code],
                {"df_rnd_services": self.context.rnd_services},
            ),
            (
                AdminHandler,
                [],
                {
                    "company_code": self.context.company_code,
                    "df_royalty": self.context.royalties,
                },
            ),
            (
                RecalculateTotals,
                [self.context.otp_pnl],
                {"company_code": self.context.company_code},
            ),
        ]

        for i, (handler_cls, extra_args, extra_kwargs) in enumerate(handler_configs):
            # First argument is always the dataframe
            df_to_use = self.df if i == 0 else self.df_ok

            # Instantiate with df + any extra parameters
            handler = handler_cls(df_to_use, *extra_args, **extra_kwargs)
            self.logger.debug(
                "handler_processing_started",
                pipeline=type(self).__name__,
                handler=handler_cls.__name__,
            )
            processed_df = handler.process()
            self.df_ok = processed_df
            self.logger.debug(
                "handler_processing_completed",
                pipeline=type(self).__name__,
                handler=type(handler).__name__,
            )

        self.logger.info(
            "pipeline_processing_completed",
            pipeline=type(self).__name__,
            final_shape=self.df_ok.shape,
        )
        return self.df_ok
