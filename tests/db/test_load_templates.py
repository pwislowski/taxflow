from decimal import Decimal

import pandas as pd
from sqlmodel import Session, SQLModel, create_engine, select

from app.db import models
from app.db.load_templates import (
    _line_item_label,
    _to_decimal,
    _to_int,
    build_load_context,
    load_grand_totals,
    prepare_company_codes,
    prepare_line_items,
)
from app.enums import HighLevelSegmentedPnlColumns, SapBwColumns


def _session() -> Session:
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)
    return Session(engine)


def test_to_decimal_handles_none_and_nan():
    assert _to_decimal(None) == Decimal("0")
    assert _to_decimal(float("nan")) == Decimal("0")
    assert _to_decimal("10.5") == Decimal("10.5")


def test_to_int_handles_none_nan_and_default():
    assert _to_int(None) == 0
    assert _to_int(float("nan"), default=7) == 7
    assert _to_int("42") == 42


def test_line_item_label_handles_enum_and_string():
    assert (
        _line_item_label(HighLevelSegmentedPnlColumns.TotalEBIT)
        == HighLevelSegmentedPnlColumns.TotalEBIT.value
    )
    assert _line_item_label("Custom Item") == "Custom Item"


def test_prepare_company_codes_and_line_items_create_missing_rows():
    with _session() as session:
        company_map = prepare_company_codes(session, ["AA01", "BB02"])
        line_item_map = prepare_line_items(session, ["Item A", "Item B"])

        assert set(company_map.keys()) == {"AA01", "BB02"}
        assert set(line_item_map.keys()) == {"Item A", "Item B"}

        companies = session.exec(select(models.CompanyCode)).all()
        line_items = session.exec(select(models.LineItem)).all()
        assert {c.code for c in companies} == {"AA01", "BB02"}
        assert {li.name for li in line_items} == {"Item A", "Item B"}


def test_build_load_context_creates_id_mappings():
    df = pd.DataFrame(
        {
            "company_code": ["AA01", "BB02"],
            "line_item": ["Item A", "Item B"],
        }
    )

    with _session() as session:
        ctx = build_load_context(session, df, "company_code", "line_item")

        assert set(ctx.company_code_id.keys()) == {"AA01", "BB02"}
        assert set(ctx.line_item_id.keys()) == {"Item A", "Item B"}


def test_load_grand_totals_inserts_rows_and_returns_count():
    df = pd.DataFrame(
        {
            SapBwColumns.CompanyCode.value: ["AA01", "AA01"],
            SapBwColumns.PnlItem.value: [
                HighLevelSegmentedPnlColumns.TotalEBIT,
                HighLevelSegmentedPnlColumns.TotalSAR,
            ],
            SapBwColumns.Amount.value: [100, 50],
        }
    )

    with _session() as session:
        rows_loaded = load_grand_totals(session, df)

        assert rows_loaded == 2
        results = session.exec(select(models.GrandTotal)).all()
        assert len(results) == 2
        assert {row.amount for row in results} == {Decimal("100"), Decimal("50")}
