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
    UnusualItemsHandler,
    VaInventoryReceivablesHandler,
    VarianceHandler,
)
from app.interfaces import SegmentationHandler

from .base import BasePipeline
from .context import PipelineContext


class SingleEconomicActivityPipeline(BasePipeline):
    """Pipeline implementation for routine only segmentation.

    This class chains multiple segmentation handlers in a fixed sequence to process the input DataFrame.

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

    def process(self) -> pd.DataFrame:
        """Process the input DataFrame by sequentially applying segmentation handlers.

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
            (
                GsDivbuHandler,
                [
                    self.context.company_code,
                    self.context.gsdivbu_model,
                    self.context.gsdivbu_charges,
                ],
                {
                    "isin": [
                        Category.ContractManufacturing,
                        Category.Distribution,
                        Category.OwnManufacturingIC,
                        Category.OwnManufacturingThirdParty,
                    ]
                },
            ),
            (SalesMarketingHandler, [], {}),
            (AdminHandler, [], {}),
            (
                RndHandler,
                [self.context.company_code],
                {
                    "isin": [
                        Category.ContractManufacturing,
                        Category.Distribution,
                        Category.OwnManufacturingIC,
                        Category.OwnManufacturingThirdParty,
                    ]
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
