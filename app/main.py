# ruff: noqa E402
import warnings
from typing import TYPE_CHECKING, List

import pandas as pd
import structlog

from app.logging import configure_logging

warnings.filterwarnings(
    "ignore",
    message="Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated.*",
    category=FutureWarning,
)
warnings.filterwarnings("ignore", module="pandas")

from app.config import (
    app_config,
    company_config,
    pipeline_config,
    segment_config,
)
from app.enums import OtpSegmentedPnlColumns
from app.etl.loaders.grand_totals import GrandTotalETL
from app.etl.loaders.gs_divbu_charges import GsDivBuChargesETL
from app.etl.loaders.gs_divbu_model import GsDivbuModelETL
from app.etl.loaders.otp_segmented_pnl import OtpSegmentedPnlETL
from app.etl.loaders.rnd_service import RndServiceETL
from app.etl.loaders.royalties import RoyaltiesETL
from app.pipelines import PipelineFactory
from app.pipelines.context import PipelineContext


def _build_pipeline_context(
    logger,
) -> tuple[PipelineContext, pd.DataFrame]:
    etl_grand_total = GrandTotalETL(
        app_config.data_sources.grand_total.fpath,
        sheet_name=app_config.data_sources.grand_total.sheet_name,
    )
    logger.info("grand_total_etl_instantiated")
    df_grand_totals = etl_grand_total.transform()
    logger.info("grand_totals_transformed", rows=len(df_grand_totals))

    etl_royalties = RoyaltiesETL(
        app_config.data_sources.royalties,
        app_config.data_sources.royalties.fpath,
        sheet_name=app_config.data_sources.royalties.sheet_name,
    )
    logger.info("royalties_etl_instantiated")
    df_royalties = etl_royalties.transform()
    logger.info("royalties_transformed", rows=len(df_royalties))

    etl_gsdivbu_charges = GsDivBuChargesETL(
        app_config.data_sources.gs_charges,
        app_config.data_sources.gs_charges.fpath,
        sheet_name=app_config.data_sources.gs_charges.sheet_name,
    )
    logger.info("gs_divbu_charges_etl_instantiated")
    df_gsdivbu_charges = etl_gsdivbu_charges.transform()
    logger.info("gs_divbu_charges_transformed", rows=len(df_gsdivbu_charges))

    etl_otp_pnl = OtpSegmentedPnlETL(
        app_config.data_sources.otp_segmented_pnl.fpath,
        app_config.data_sources.otp_segmented_pnl.sheet_name,
    )
    logger.info("otp_segmented_pnl_etl_instantiated")
    df_otp = etl_otp_pnl.transform()
    logger.info("otp_transformed", rows=len(df_otp))

    etl_gsdivbu_model = GsDivbuModelETL(app_config.data_sources.shared_services)
    logger.info("gs_divbu_model_etl_instantiated")
    df_gsdivbu_model = etl_gsdivbu_model.transform()
    logger.info("gs_divbu_model_transformed", rows=len(df_gsdivbu_model))

    etl_rnd_services = RndServiceETL(app_config.data_sources.rnd_services)
    logger.info("rnd_services_etl_instantiated")
    df_rnd_services = etl_rnd_services.transform()
    logger.info("rnd_services_transformed", rows=len(df_rnd_services))

    context = PipelineContext(
        grand_totals=df_grand_totals,
        royalties=df_royalties,
        otp_pnl=df_otp,
        gsdivbu_model=etl_gsdivbu_model,
        gsdivbu_charges=etl_gsdivbu_charges,
        gsdivbu_charges_df=df_gsdivbu_charges,
        rnd_services=df_rnd_services,
    )
    logger.info("pipeline_context_created")

    df_otp_enhanced = etl_otp_pnl.transform_and_enhance(segment_config)
    return context, df_otp_enhanced


def run_with_context(
    company_codes: List[str], debug: bool
) -> tuple[pd.DataFrame, PipelineContext]:
    """Run the OTP segmentation processing pipeline and return context."""
    level = "DEBUG" if debug or app_config.debug else app_config.log_level
    configure_logging(app_config, level=level)
    logger = structlog.get_logger(__name__)
    company_codes = [x.upper() for x in company_codes]

    logger.info("loaded_config", source="app.config", log_level=level)

    try:
        context, df_otp_enhanced = _build_pipeline_context(logger)

        df_ok = pd.DataFrame()
        company_config_lookup = {
            conf.company_code.upper(): conf for conf in company_config
        }

        for company_code in company_codes:
            logger.info("starting_company_processing", company_code=company_code)
            context.relevant_gsdiv_bus = None
            context.external_cost_business_units = None
            context.target_ebit_erosion = None

            context.company_code = company_code
            structlog.contextvars.bind_contextvars(company_code=company_code)
            company_conf = company_config_lookup.get(company_code)
            if company_conf:
                context.relevant_gsdiv_bus = company_conf.shared_costs_business_units
                if context.relevant_gsdiv_bus:
                    logger.info(
                        "loaded_relevant_gsdiv_bus",
                        company_code=company_code,
                        relevant_gsdiv_bus=context.relevant_gsdiv_bus,
                    )
                context.external_cost_business_units = (
                    company_conf.external_costs_business_units
                )
                if context.external_cost_business_units:
                    logger.info(
                        "loaded_external_cost_business_units",
                        company_code=company_code,
                        external_cost_business_units=context.external_cost_business_units,
                    )
                context.target_ebit_erosion = company_conf.target_ebit_erosion
                if context.target_ebit_erosion is not None:
                    logger.info(
                        "loaded_target_ebit_erosion",
                        company_code=company_code,
                        target_ebit_erosion=context.target_ebit_erosion,
                    )

            if TYPE_CHECKING:
                assert isinstance(context.otp_pnl, pd.DataFrame)

            dfc = df_otp_enhanced.copy()
            dfc = dfc[dfc[OtpSegmentedPnlColumns.CompanyCode] == company_code]

            if TYPE_CHECKING:
                assert isinstance(dfc, pd.DataFrame)

            pipeline = PipelineFactory.build_pipeline(dfc, context, pipeline_config)
            logger.info("built_pipeline", company_code=company_code)

            processed = pipeline.process()
            logger.info(
                "pipeline_processed",
                company_code=company_code,
                rows=len(processed),
            )
            df_ok = pd.concat([df_ok, processed], axis=0)

        logger.info("completed_company_processing", company_codes=company_codes)

        # Exclude processed company codes from the temp dataframe
        processed_codes = set(company_codes)
        temp = df_otp_enhanced[
            ~df_otp_enhanced[OtpSegmentedPnlColumns.CompanyCode].isin(processed_codes)  # type:ignore
        ]

        res = pd.concat([temp, df_ok], ignore_index=True)

        if TYPE_CHECKING:
            assert isinstance(res, pd.DataFrame)

        return res, context

    except Exception as e:
        logger.error("pipeline_execution_failed", error=str(e), exc_info=debug)
        raise


def run(company_codes: List[str], debug: bool) -> pd.DataFrame:
    """Run the OTP segmentation processing pipeline."""
    res, _ = run_with_context(company_codes, debug)
    return res
