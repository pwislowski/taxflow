import pytest
from pydantic import ValidationError
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

from app.config import AppConfig, SegmentConfig


class AppConfigForTests(AppConfig):
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


def test_load_segment_configs(sample_segment_configs):
    """Test that segment configs are valid."""
    configs = sample_segment_configs
    assert len(configs) > 0  # Ensure we have data
    assert all(isinstance(c, SegmentConfig) for c in configs)

    # Check a known config from the sample
    ab01_configs = [c for c in configs if c.company_code == "AB01"]
    assert len(ab01_configs) > 0
    first_ab01 = ab01_configs[0]
    assert first_ab01.segment.value == "DIS"
    assert first_ab01.activity.value == "routine"
    assert first_ab01.category.value == "distribution"


def test_app_config_loading(sample_app_config_data):
    """Test that AppConfig loads from data correctly."""
    config = AppConfigForTests.model_validate(sample_app_config_data)
    assert config.debug is True
    assert hasattr(config, "data_sources")

    # Test nested structures
    ds = config.data_sources
    assert hasattr(ds, "shared_services")
    assert isinstance(ds.rnd_services, list)
    assert len(ds.rnd_services) > 0
    assert hasattr(ds.rnd_services[0], "company_code")


def test_invalid_segment_config_validation():
    """Test that invalid data raises ValidationError."""
    # Invalid segment
    with pytest.raises(ValidationError, match="Input should be"):
        SegmentConfig.model_validate(
            {
                "company_code": "TEST",
                "segment": "INVALID_SEGMENT",
                "activity": "routine",
                "category": "distribution",
            }
        )

    # Invalid activity
    with pytest.raises(ValidationError):
        SegmentConfig.model_validate(
            {
                "company_code": "TEST",
                "segment": "DIS",
                "activity": "invalid_activity",
                "category": "distribution",
            }
        )


def test_database_path_creates_parent_dirs(sample_app_config_data, tmp_path):
    db_path = tmp_path / "nested" / "app.db"
    assert not db_path.parent.exists()

    config = AppConfigForTests.model_validate(
        {
            **sample_app_config_data,
            "database_path": db_path,
        }
    )

    assert config.database_path == db_path
    assert db_path.parent.exists()
