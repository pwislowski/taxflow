import csv
from pathlib import Path
from typing import Annotated, List, Optional, Sequence

from pydantic import BaseModel, StringConstraints, field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

from .enums import Activity, Category, Segment

type UpperCaseString = Annotated[str, StringConstraints(to_upper=True)]


class SegmentConfig(BaseModel):
    model_config = SettingsConfigDict(
        extra="ignore",
    )

    company_code: UpperCaseString
    segment: Segment
    activity: Activity
    category: Category


class PipelineConfig(BaseSettings):
    model_config = SettingsConfigDict(
        yaml_file="segmentation_config.yaml",
        yaml_file_encoding="utf-8",
        extra="ignore",
    )

    mixed_activity: Sequence[UpperCaseString] = []
    mixed_activity_with_external_costs: Sequence[UpperCaseString] = []
    single_activity: Sequence[UpperCaseString] = []
    manual_erosion_entities: Sequence[UpperCaseString] = []

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        sources = (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )
        try:
            yaml_source = YamlConfigSettingsSource(
                settings_cls, yaml_config_section="pipeline"
            )
        except KeyError:
            return sources
        return (yaml_source, *sources)


class RoyaltiesConfig(BaseModel):
    fpath: Path
    sheet_name: str
    accounts_in_scope: Sequence[str]


class CompanyConfig(BaseModel):
    company_code: UpperCaseString
    shared_costs_business_units: Optional[Sequence[UpperCaseString]] = None
    external_costs_business_units: Optional[Sequence[UpperCaseString]] = None
    target_ebit_erosion: Optional[float] = None


class CompanyConfigs(BaseSettings):
    model_config = SettingsConfigDict(
        yaml_file="segmentation_config.yaml",
        yaml_file_encoding="utf-8",
        extra="ignore",
    )

    company_configs: Sequence[CompanyConfig]

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        sources = (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )
        yaml_source = YamlConfigSettingsSource(settings_cls)
        return (yaml_source, *sources)


class FileConfig(BaseModel):
    fpath: Path
    sheet_name: str = "Sheet1"


class SharedServicesConfig(BaseModel):
    divbu_income: FileConfig
    divbu_expense: FileConfig
    gs_income: FileConfig
    gs_expense: FileConfig


class RndServicesConfig(BaseModel):
    company_code: str
    fpath: Path
    sheet_name: str = "Sheet1"
    accounts_in_scope: Sequence[int]


class GsChargesConfig(BaseModel):
    fpath: Path
    sheet_name: str = "Sheet1"
    accounts_in_scope: Sequence[str]


class DataSourcesConfig(BaseModel):
    shared_services: SharedServicesConfig
    otp_segmented_pnl: FileConfig
    grand_total: FileConfig
    royalties: RoyaltiesConfig
    gs_charges: GsChargesConfig
    rnd_services: Sequence[RndServicesConfig]


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(
        yaml_file="config.yaml",
        yaml_file_encoding="utf-8",
        extra="ignore",
    )

    debug: bool = False
    data_sources: DataSourcesConfig

    database_path: Path = Path("./local/app.db")
    db_echo: bool = False

    @field_validator("database_path")
    @classmethod
    def ensure_database_dir_exists(cls, value: Path) -> Path:
        parent = value.parent
        if parent != Path(".") and not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
        return value

    log_level: str = "INFO"
    log_to_console: bool = True

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        sources = (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )
        yaml_source = YamlConfigSettingsSource(settings_cls)
        return (yaml_source, *sources)


def load_segment_configs(
    csv_path=Path("segments.csv"),
) -> List[SegmentConfig]:
    """Load segment configurations from CSV file."""
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        segment_config = [SegmentConfig.model_validate(row) for row in reader]

    return segment_config


app_config = AppConfig()  # type: ignore
pipeline_config = PipelineConfig()
segment_config = load_segment_configs()
company_config = CompanyConfigs().company_configs  # type: ignore
