from pathlib import Path

import pytest

from app.config import SegmentConfig
from app.enums import Activity, Category, Segment


@pytest.fixture
def sample_segment_configs():
    """Fixture providing sample segment configurations for testing."""
    return [
        SegmentConfig(
            company_code="AB01",
            segment=Segment.Distribution,
            activity=Activity.Routine,
            category=Category.Distribution,
        ),
        SegmentConfig(
            company_code="AB02",
            segment=Segment.Other,
            activity=Activity.Entrepreneur,
            category=Category.OwnManufacturingThirdParty,
        ),
    ]


@pytest.fixture
def sample_app_config_data():
    """Fixture providing sample data for AppConfig."""
    return {
        "debug": True,
        "data_sources": {
            "shared_services": {
                "divbu_income": {
                    "fpath": Path("/tmp/test.xlsx"),
                    "sheet_name": "Sheet1",
                },
                "divbu_expense": {
                    "fpath": Path("/tmp/test.xlsx"),
                    "sheet_name": "Sheet1",
                },
                "gs_income": {
                    "fpath": Path("/tmp/test.xlsx"),
                    "sheet_name": "Sheet1",
                },
                "gs_expense": {
                    "fpath": Path("/tmp/test.xlsx"),
                    "sheet_name": "Sheet1",
                },
            },
            "otp_segmented_pnl": {
                "fpath": Path("/tmp/test.xlsx"),
                "sheet_name": "Sheet1",
            },
            "grand_total": {
                "fpath": Path("/tmp/test.xlsx"),
                "sheet_name": "Sheet1",
            },
            "royalties": {
                "fpath": Path("/tmp/test.xlsx"),
                "sheet_name": "Sheet1",
                "accounts_in_scope": ["427799"],
            },
            "gs_charges": {
                "fpath": Path("/tmp/test.xlsx"),
                "sheet_name": "Sheet1",
                "accounts_in_scope": ["427810"],
            },
            "rnd_services": [
                {
                    "company_code": "TEST",
                    "fpath": Path("/tmp/test.xlsx"),
                    "sheet_name": "Sheet1",
                    "accounts_in_scope": [1, 2, 3],
                }
            ],
        },
    }
