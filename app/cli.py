from typing import Annotated, Sequence

import pandas as pd
from pydantic import AliasChoices, BaseModel, ConfigDict, Field
from pydantic_settings import (
    CliApp,
    CliImplicitFlag,
    CliPositionalArg,
    CliSubCommand,
)

from app.config import app_config, pipeline_config
from app.db.load_templates import (
    load_grand_totals,
    load_otp_segmented_pnl,
    load_otp_unsegmented_pnl,
    load_rnd_service,
    load_royalties,
    load_shared_services_total_charge,
)
from app.db.session import create_db_and_tables, session_scope
from app.etl.loaders.grand_totals import GrandTotalETL
from app.etl.loaders.gs_divbu_charges import GsDivBuChargesETL
from app.etl.loaders.otp_segmented_pnl import OtpSegmentedPnlETL
from app.etl.loaders.rnd_service import RndServiceETL
from app.etl.loaders.royalties import RoyaltiesETL
from app.main import run, run_with_context
from app.pipelines.context import PipelineContext


def _load_db_from_sources(
    df_segmented_pnl: pd.DataFrame,
    context: PipelineContext | None = None,
) -> dict[str, int]:
    create_db_and_tables()
    if (
        context is None
        or context.grand_totals is None
        or context.royalties is None
        or context.gsdivbu_charges_df is None
        or context.rnd_services is None
        or context.otp_pnl is None
    ):
        etl_grand_total = GrandTotalETL(
            app_config.data_sources.grand_total.fpath,
            sheet_name=app_config.data_sources.grand_total.sheet_name,
        )
        df_grand_totals = etl_grand_total.transform()

        etl_royalties = RoyaltiesETL(
            app_config.data_sources.royalties,
            app_config.data_sources.royalties.fpath,
            sheet_name=app_config.data_sources.royalties.sheet_name,
        )
        df_royalties = etl_royalties.transform()

        etl_gsdivbu_charges = GsDivBuChargesETL(
            app_config.data_sources.gs_charges,
            app_config.data_sources.gs_charges.fpath,
            sheet_name=app_config.data_sources.gs_charges.sheet_name,
        )
        df_gsdivbu_charges = etl_gsdivbu_charges.transform()

        etl_rnd_services = RndServiceETL(app_config.data_sources.rnd_services)
        df_rnd_services = etl_rnd_services.transform()

        etl_otp_pnl = OtpSegmentedPnlETL(
            app_config.data_sources.otp_segmented_pnl.fpath,
            app_config.data_sources.otp_segmented_pnl.sheet_name,
        )
        df_otp = etl_otp_pnl.transform()
    else:
        df_grand_totals = context.grand_totals
        df_royalties = context.royalties
        df_gsdivbu_charges = context.gsdivbu_charges_df
        df_rnd_services = context.rnd_services
        df_otp = context.otp_pnl

    with session_scope() as session:
        return {
            "grand_totals": load_grand_totals(session, df_grand_totals),
            "royalties": load_royalties(session, df_royalties),
            "shared_services_total_charge": load_shared_services_total_charge(
                session, df_gsdivbu_charges
            ),
            "rnd_service": load_rnd_service(session, df_rnd_services),
            "product_business": load_otp_unsegmented_pnl(session, df_otp),
            "product_business_segmented": load_otp_segmented_pnl(
                session, df_segmented_pnl
            ),
        }


class All(BaseModel):
    model_config = ConfigDict(extra="allow")
    load_db: Annotated[
        CliImplicitFlag[bool],
        Field(
            validation_alias=AliasChoices("load-db"),
        ),
    ] = False

    def cli_cmd(self) -> None:
        companies = sorted(
            {
                *pipeline_config.mixed_activity,
                *pipeline_config.mixed_activity_with_external_costs,
                *pipeline_config.single_activity,
                *pipeline_config.manual_erosion_entities,
            }
        )
        context = PipelineContext()
        if self.load_db:
            result, context = run_with_context(companies, debug=False)
        else:
            result = run(companies, debug=False)
        print(
            f"Processed {len(companies)} companies; rows={len(result)} cols={len(result.columns)}"
        )
        if self.load_db:
            rows_loaded = _load_db_from_sources(result, context)
            total_rows = sum(rows_loaded.values())
            print(f"Loaded {total_rows} rows into database")
            for name, count in rows_loaded.items():
                print(f"  {name}: {count}")


class Subset(BaseModel):
    companies: CliPositionalArg[Sequence[str]]
    load_db: Annotated[
        CliImplicitFlag[bool],
        Field(
            validation_alias=AliasChoices("load-db"),
        ),
    ] = False

    def cli_cmd(self) -> None:
        context = PipelineContext()
        if self.load_db:
            result, context = run_with_context(list(self.companies), debug=False)
        else:
            result = run(list(self.companies), debug=False)
        print(
            f"Processed {len(self.companies)} companies; rows={len(result)} cols={len(result.columns)}"
        )
        if self.load_db:
            rows_loaded = _load_db_from_sources(result, context)
            total_rows = sum(rows_loaded.values())
            print(f"Loaded {total_rows} rows into database")
            for name, count in rows_loaded.items():
                print(f"  {name}: {count}")


class Taxflow(BaseModel, cli_prog_name="taxflow"):
    all: CliSubCommand[All]
    subset: CliSubCommand[Subset]

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)


cmd = CliApp.run(Taxflow)
