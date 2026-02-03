import pandas as pd
import structlog

from app.enums import OtpSegmentedPnlColumns
from app.interfaces import Pipeline

from .context import PipelineContext


class BasePipeline(Pipeline):
    def __init__(self, df: pd.DataFrame, context: PipelineContext) -> None:
        self.logger = structlog.get_logger(f"{__name__}.{type(self).__name__}")
        self.df = df

        assert isinstance(context.company_code, str)
        self.company_code = context.company_code

        self.df_ok = pd.DataFrame()
        self.logger.info("pipeline_initialized", company_code=self.company_code)
        self.logger.debug("input_dataframe_shape", shape=self.df.shape)
        self._check()

    def process(self) -> pd.DataFrame: ...

    def _check(self) -> None:
        self.logger.debug("validation_checks_started")
        uniqs = self.df[OtpSegmentedPnlColumns.CompanyCode].unique()

        if len(uniqs) != 1:
            self.logger.error(
                "multiple_company_codes_in_dataframe",
                company_codes=uniqs,
            )
        assert len(uniqs) == 1, (
            f"Dataframe passed is not unique for the entity - {uniqs}"
        )

        if uniqs[0].upper() != self.company_code.upper():
            self.logger.error(
                "company_code_mismatch",
                dataframe_company_code=uniqs[0],
                expected_company_code=self.company_code,
            )
        assert uniqs[0].upper() == self.company_code.upper(), (
            f"Dataframe passed is not reflective of the company_code passed - {uniqs[0]} != {self.company_code}"
        )

        self.logger.info("validation_checks_passed")
