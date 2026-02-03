# ruff: noqa F402
import pandas as pd
import pytest
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

from app.config import PipelineConfig


class _PipelineConfigStub(PipelineConfig):
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings,)


from app.enums import OtpSegmentedPnlColumns
from app.pipelines.complex import (
    EntrepreneurWithRoutineAndExternalCosts,
    EntrepreneurWithRoutinePipeline,
)
from app.pipelines.context import PipelineContext
from app.pipelines.cost_adjustment_pipeline import CostAdjustmentPipeline
from app.pipelines.factory import PipelineFactory
from app.pipelines.single_economic_activity import SingleEconomicActivityPipeline


def _sample_df(company_code: str = "AA01") -> pd.DataFrame:
    return pd.DataFrame(
        {
            OtpSegmentedPnlColumns.CompanyCode: [company_code],
        }
    )


def test_factory_builds_external_costs_pipeline():
    config = _PipelineConfigStub(
        mixed_activity_with_external_costs=["AA01"],
        mixed_activity=[],
        single_activity=[],
        manual_erosion_entities=[],
    )
    context = PipelineContext(company_code="AA01")
    df = _sample_df("AA01")

    pipeline = PipelineFactory.build_pipeline(df, context, config)

    assert isinstance(pipeline, EntrepreneurWithRoutineAndExternalCosts)


def test_factory_builds_manual_erosion_pipeline():
    config = _PipelineConfigStub(
        mixed_activity_with_external_costs=[],
        mixed_activity=[],
        single_activity=[],
        manual_erosion_entities=["AA01"],
    )
    context = PipelineContext(company_code="AA01")
    df = _sample_df("AA01")

    pipeline = PipelineFactory.build_pipeline(df, context, config)

    assert isinstance(pipeline, CostAdjustmentPipeline)


def test_factory_builds_mixed_activity_pipeline():
    config = _PipelineConfigStub(
        mixed_activity_with_external_costs=[],
        mixed_activity=["AA01"],
        single_activity=[],
        manual_erosion_entities=[],
    )
    context = PipelineContext(company_code="AA01")
    df = _sample_df("AA01")

    pipeline = PipelineFactory.build_pipeline(df, context, config)

    assert isinstance(pipeline, EntrepreneurWithRoutinePipeline)


def test_factory_builds_single_activity_pipeline():
    config = _PipelineConfigStub(
        mixed_activity_with_external_costs=[],
        mixed_activity=[],
        single_activity=["AA01"],
        manual_erosion_entities=[],
    )
    context = PipelineContext(company_code="AA01")
    df = _sample_df("AA01")

    pipeline = PipelineFactory.build_pipeline(df, context, config)

    assert isinstance(pipeline, SingleEconomicActivityPipeline)


def test_factory_raises_for_unknown_company_code():
    config = _PipelineConfigStub(
        mixed_activity_with_external_costs=[],
        mixed_activity=[],
        single_activity=[],
        manual_erosion_entities=[],
    )
    context = PipelineContext(company_code="AA01")
    df = _sample_df("AA01")

    with pytest.raises(NotImplementedError, match="has not been yet implemented"):
        PipelineFactory.build_pipeline(df, context, config)
