import pandas as pd

from app.config import PipelineConfig
from app.interfaces import Pipeline

from .complex import (
    EntrepreneurWithRoutineAndExternalCosts,
    EntrepreneurWithRoutinePipeline,
)
from .context import PipelineContext
from .cost_adjustment_pipeline import CostAdjustmentPipeline
from .single_economic_activity import (
    SingleEconomicActivityPipeline,
)


class PipelineFactory:
    """Factory class for creating pipeline instances based on company code.

    This class provides a static method to instantiate the appropriate pipeline
    subclass depending on the company code in the PipelineContext. It supports
    specific company codes and raises an error for unsupported ones.
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def build_pipeline(
        df: pd.DataFrame,
        context: PipelineContext,
        config: PipelineConfig,
    ) -> Pipeline:
        """Build and return a specific pipeline instance based on the company code in the context.

        Args:
            df: The input DataFrame to process.
            context: The PipelineContext containing company code and other resources.
            config: The PipelineConfig specifying company code to pipeline mappings.

        Returns:
            An instance of Pipeline subclass appropriate for the company code.

        Raises:
            AssertionError: If company_code in context is not a string.
            NotImplementedError: If the company code is not supported.
        """
        assert isinstance(context.company_code, str)
        entity = context.company_code.upper()

        if entity in config.mixed_activity_with_external_costs:
            return EntrepreneurWithRoutineAndExternalCosts(df, context)

        elif entity in config.manual_erosion_entities:
            return CostAdjustmentPipeline(df, context)

        elif entity in config.mixed_activity:
            return EntrepreneurWithRoutinePipeline(df, context)

        elif entity in config.single_activity:
            return SingleEconomicActivityPipeline(df, context)

        else:
            raise NotImplementedError(
                f"Company code {context.company_code} has not been yet implemented"
            )
