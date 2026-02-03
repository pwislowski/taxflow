from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Type

import pandas as pd
import structlog
from sqlmodel import Session, select

from app.db import models
from app.enums import HighLevelSegmentedPnlColumns, OtpSegmentedPnlColumns, SapBwColumns

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class LoadContext:
    """Precomputed ID mappings used during bulk loads."""

    company_code_id: Dict[str, int]
    line_item_id: Dict[str, int]


def _to_decimal(value: object) -> Decimal:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return Decimal("0")
    return Decimal(str(value))


def _to_int(value: object, default: int = 0) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    return int(str(value))


def _line_item_label(value: object) -> str:
    if isinstance(value, HighLevelSegmentedPnlColumns):
        return value.value
    return str(value)


def prepare_company_codes(
    session: Session,
    company_codes: Iterable[str],
) -> Dict[str, int]:
    """Ensure company codes exist; return mapping of code -> id."""
    company_codes = set(
        [c for c in company_codes if c != OtpSegmentedPnlColumns.CompanyCode]
    )

    stmt = select(models.CompanyCode)
    existing = session.exec(stmt).all()
    missing: List[models.CompanyCode] = []
    for company_code in company_codes:
        if company_code not in [c.code for c in existing]:
            add = models.CompanyCode(code=company_code)
            missing.append(add)

    session.add_all(missing)
    session.commit()

    for company_code in missing:
        session.refresh(company_code)

    return {c.code: c.id for c in [*existing, *missing]}  # type:ignore


def prepare_line_items(
    session: Session,
    line_item_names: Iterable[str],
) -> Dict[str, int]:
    """Ensure line items exist; return mapping of name -> id."""
    unique_names = {name.strip() for name in line_item_names if name}

    stmt = select(models.LineItem)
    existing = session.exec(stmt).all()
    missing: List[models.LineItem] = []
    for line_item_name in unique_names:
        if line_item_name not in [li.name for li in existing]:
            missing.append(models.LineItem(name=line_item_name))

    session.add_all(missing)
    session.commit()

    for line_item in missing:
        session.refresh(line_item)

    return {li.name: li.id for li in [*existing, *missing]}  # type:ignore


def build_load_context(
    session: Session,
    df: pd.DataFrame,
    company_code_col: str,
    line_item_col: str,
) -> LoadContext:
    """Create ID mappings needed for segmented P&L loads."""
    company_code_id = prepare_company_codes(
        session, df[company_code_col].dropna().astype(str)
    )
    line_item_id = prepare_line_items(session, df[line_item_col].dropna().astype(str))
    return LoadContext(company_code_id=company_code_id, line_item_id=line_item_id)


def _otp_high_level_line_item_map() -> Dict[str, str]:
    return {
        OtpSegmentedPnlColumns.Net_Sales_Total.value: HighLevelSegmentedPnlColumns.TotalNetSales.value,
        OtpSegmentedPnlColumns.Cogs.value: HighLevelSegmentedPnlColumns.Cogs.value,
        OtpSegmentedPnlColumns.Gross_Profit_Before_Variances.value: HighLevelSegmentedPnlColumns.GrossProfitBeforeVariances.value,
        OtpSegmentedPnlColumns.Gross_Profit_After_Variances.value: HighLevelSegmentedPnlColumns.GrossProfitAfterVariances.value,
        OtpSegmentedPnlColumns.Total_SAR.value: HighLevelSegmentedPnlColumns.TotalSAR.value,
        OtpSegmentedPnlColumns.Ebit.value: HighLevelSegmentedPnlColumns.TotalEBIT.value,
    }


def load_otp_segmented_pnl(
    session: Session,
    df: pd.DataFrame,
    company_code_col: str = OtpSegmentedPnlColumns.CompanyCode.value,
    category_col: Optional[str] = OtpSegmentedPnlColumns.Category.value,
    table_model: Type[
        models.ProductBusinessSegmented
    ] = models.ProductBusinessSegmented,
) -> int:
    """Load OTP segmented P&L data for all numeric columns."""
    logger.debug("load_otp_segmented_pnl_started", rows=len(df))
    line_item_map = _otp_high_level_line_item_map()
    company_code_id = prepare_company_codes(
        session, df[company_code_col].dropna().astype(str)
    )
    line_item_id = prepare_line_items(session, line_item_map.values())
    ctx = LoadContext(company_code_id=company_code_id, line_item_id=line_item_id)
    numeric_cols = [
        *OtpSegmentedPnlColumns.list_numeric_cols(),
        *OtpSegmentedPnlColumns.list_percentage_cols(),
    ]

    rows = []
    for _, row in df.iterrows():
        company_code = str(row[company_code_col]).upper()
        company_id = ctx.company_code_id.get(company_code)
        if company_id is None:
            continue

        category = None
        if category_col:
            category = None if pd.isna(row[category_col]) else str(row[category_col])  # type: ignore

        for otp_col in numeric_cols:
            line_item_name = line_item_map.get(otp_col, otp_col)
            amount = _to_decimal(row.get(otp_col))
            line_item_id = ctx.line_item_id.get(line_item_name)

            rows.append(
                table_model(
                    company_code_id=company_id,
                    line_item_name=line_item_name,
                    line_item_id=line_item_id,
                    category=category,
                    amount=amount,
                )
            )

    session.add_all(rows)
    session.commit()
    rows_loaded = len(rows)
    logger.debug("load_otp_segmented_pnl_finished", rows=rows_loaded)
    logger.info("load_otp_segmented_pnl_total", rows=rows_loaded)
    return rows_loaded


def load_grand_totals(
    session: Session,
    df: pd.DataFrame,
    company_code_col: str = SapBwColumns.CompanyCode.value,
    line_item_col: str = SapBwColumns.PnlItem.value,
    amount_col: str = SapBwColumns.Amount.value,
    table_model: Type[models.GrandTotal] = models.GrandTotal,
) -> int:
    """Load grand totals data."""
    logger.debug("load_grand_totals_started", rows=len(df))
    company_code_id = prepare_company_codes(
        session, df[company_code_col].dropna().astype(str)
    )
    line_item_names = [_line_item_label(x) for x in df[line_item_col].dropna()]
    line_item_id = prepare_line_items(session, line_item_names)

    rows = []
    for _, row in df.iterrows():
        company_code = str(row[company_code_col]).upper()
        company_id = company_code_id.get(company_code)
        if company_id is None:
            continue

        line_item_name = _line_item_label(row[line_item_col])
        item_id = line_item_id.get(line_item_name)
        if item_id is None:
            continue

        amount = _to_decimal(row[amount_col])

        rows.append(
            table_model(
                company_code_id=company_id,
                line_item_id=item_id,
                amount=amount,
            )
        )

    session.add_all(rows)
    session.commit()
    rows_loaded = len(rows)
    logger.debug("load_grand_totals_finished", rows=rows_loaded)
    logger.info("load_grand_totals_total", rows=rows_loaded)
    return rows_loaded


def load_rnd_service(
    session: Session,
    df: pd.DataFrame,
    company_code_col: str = SapBwColumns.CompanyCode.value,
    line_item_col: str = SapBwColumns.PnlItem.value,
    amount_col: str = SapBwColumns.Amount.value,
    account_col: str = SapBwColumns.GlAccount.value,
    table_model: Type[models.RndService] = models.RndService,
) -> int:
    """Load R&D services data."""
    logger.debug("load_rnd_service_started", rows=len(df))
    company_code_id = prepare_company_codes(
        session, df[company_code_col].dropna().astype(str)
    )
    line_item_names = [_line_item_label(x) for x in df[line_item_col].dropna()]
    line_item_id = prepare_line_items(session, line_item_names)

    rows = []
    for _, row in df.iterrows():
        company_code = str(row[company_code_col]).upper()
        company_id = company_code_id.get(company_code)
        if company_id is None:
            continue

        line_item_name = _line_item_label(row[line_item_col])
        item_id = line_item_id.get(line_item_name)
        if item_id is None:
            continue

        amount = _to_decimal(row[amount_col])

        rows.append(
            table_model(
                company_code_id=company_id,
                line_item_id=item_id,
                amount=amount,
            )
        )

    session.add_all(rows)
    session.commit()
    rows_loaded = len(rows)
    logger.debug("load_rnd_service_finished", rows=rows_loaded)
    logger.info("load_rnd_service_total", rows=rows_loaded)
    return rows_loaded


def load_shared_services_total_charge(
    session: Session,
    df: pd.DataFrame,
    table_model: Type[
        models.SharedServicesTotalCharge
    ] = models.SharedServicesTotalCharge,
) -> int:
    """Load shared services total charges from pivoted GS charges data."""
    logger.debug("load_shared_services_total_charge_started", rows=len(df))
    company_code_id = prepare_company_codes(session, [str(x) for x in df.index])
    line_item_names = [_line_item_label(x) for x in df.columns]
    line_item_id = prepare_line_items(session, line_item_names)

    rows = []
    for company_code, row in df.iterrows():
        company_id = company_code_id.get(str(company_code).upper())
        if company_id is None:
            continue

        for col in df.columns:
            line_item_name = _line_item_label(col)
            item_id = line_item_id.get(line_item_name)
            if item_id is None:
                continue

            amount = _to_decimal(row[col])

            rows.append(
                table_model(
                    company_code_id=company_id,
                    line_item_id=item_id,
                    amount=amount,
                )
            )

    session.add_all(rows)
    session.commit()
    rows_loaded = len(rows)
    logger.debug("load_shared_services_total_charge_finished", rows=rows_loaded)
    logger.info("load_shared_services_total_charge_total", rows=rows_loaded)
    return rows_loaded


def load_royalties(
    session: Session,
    df: pd.DataFrame,
    company_code_col: str = SapBwColumns.CompanyCode.value,
    line_item_col: str = SapBwColumns.PnlItem.value,
    amount_col: str = SapBwColumns.Amount.value,
    account_col: str = SapBwColumns.GlAccount.value,
    table_model: Type[
        models.IntellectualPropertyService
    ] = models.IntellectualPropertyService,
) -> int:
    """Load intellectual property service (royalties) data."""
    logger.debug("load_royalties_started", rows=len(df))
    company_code_id = prepare_company_codes(
        session, df[company_code_col].dropna().astype(str)
    )
    line_item_names = [_line_item_label(x) for x in df[line_item_col].dropna()]
    line_item_id = prepare_line_items(session, line_item_names)

    rows = []
    for _, row in df.iterrows():
        company_code = str(row[company_code_col]).upper()
        company_id = company_code_id.get(company_code)
        if company_id is None:
            continue

        line_item_name = _line_item_label(row[line_item_col])
        item_id = line_item_id.get(line_item_name)
        if item_id is None:
            continue

        account_number = _to_int(row.get(account_col))
        amount = _to_decimal(row[amount_col])

        rows.append(
            table_model(
                company_code_id=company_id,
                line_item_id=item_id,
                account_number=account_number,
                amount=amount,
            )
        )

    session.add_all(rows)
    session.commit()
    rows_loaded = len(rows)
    logger.debug("load_royalties_finished", rows=rows_loaded)
    logger.info("load_royalties_total", rows=rows_loaded)
    return rows_loaded


def load_otp_unsegmented_pnl(
    session: Session,
    df: pd.DataFrame,
    company_code_col: str = OtpSegmentedPnlColumns.CompanyCode.value,
    category_col: Optional[str] = OtpSegmentedPnlColumns.Category.value,
    table_model: Type[models.ProductBusiness] = models.ProductBusiness,
) -> int:
    """Load OTP unsegmented P&L data for all numeric columns."""
    logger.debug("load_otp_unsegmented_pnl_started", rows=len(df))
    line_item_map = _otp_high_level_line_item_map()
    company_code_id = prepare_company_codes(
        session, df[company_code_col].dropna().astype(str)
    )
    line_item_id = prepare_line_items(session, line_item_map.values())
    numeric_cols = [
        *OtpSegmentedPnlColumns.list_numeric_cols(),
        *OtpSegmentedPnlColumns.list_percentage_cols(),
    ]

    rows = []
    for _, row in df.iterrows():
        company_code = str(row[company_code_col]).upper()
        company_id = company_code_id.get(company_code)
        if company_id is None:
            continue

        for otp_col in numeric_cols:
            line_item_name = line_item_map.get(otp_col, otp_col)
            amount = _to_decimal(row.get(otp_col))
            item_id = line_item_id.get(line_item_name)

            rows.append(
                table_model(
                    company_code_id=company_id,
                    line_item_name=line_item_name,
                    line_item_id=item_id,
                    amount=amount,
                )
            )

    session.add_all(rows)
    session.commit()
    rows_loaded = len(rows)
    logger.debug("load_otp_unsegmented_pnl_finished", rows=rows_loaded)
    logger.info("load_otp_unsegmented_pnl_total", rows=rows_loaded)
    return rows_loaded


def load_segmented_pnl(
    session: Session,
    df: pd.DataFrame,
    company_code_col: str,
    line_item_col: str,
    amount_col: str,
    category_col: Optional[str] = None,
    table_model: Type[
        models.ProductBusinessSegmented
    ] = models.ProductBusinessSegmented,
) -> int:
    """Template loader for segmented P&L data.

    - Expects one row per line item per company_code.
    - Writes into ProductBusinessSegmented by default.
    """
    logger.debug("load_segmented_pnl_started", rows=len(df))
    ctx = build_load_context(session, df, company_code_col, line_item_col)

    rows = []
    for _, row in df.iterrows():
        company_code = str(row[company_code_col]).upper()
        line_item = str(row[line_item_col])
        amount = _to_decimal(row[amount_col])

        company_id = ctx.company_code_id.get(company_code)
        if company_id is None:
            continue

        line_item_id = ctx.line_item_id.get(line_item)

        category = None
        if category_col:
            category = None if pd.isna(row[category_col]) else str(row[category_col])  # type: ignore

        rows.append(
            table_model(
                company_code_id=company_id,
                line_item_name=line_item,
                line_item_id=line_item_id,
                category=category,
                amount=amount,
            )
        )

    session.add_all(rows)
    session.commit()
    rows_loaded = len(rows)
    logger.debug("load_segmented_pnl_finished", rows=rows_loaded)
    logger.info("load_segmented_pnl_total", rows=rows_loaded)
    return rows_loaded


def load_unsegmented_pnl(
    session: Session,
    df: pd.DataFrame,
    company_code_col: str,
    line_item_col: str,
    amount_col: str,
    table_model: Type[models.ProductBusiness] = models.ProductBusiness,
) -> int:
    """Template loader for unsegmented P&L data."""
    logger.debug("load_unsegmented_pnl_started", rows=len(df))
    ctx = build_load_context(session, df, company_code_col, line_item_col)

    rows = []
    for _, row in df.iterrows():
        company_code = str(row[company_code_col]).upper()
        line_item = str(row[line_item_col])
        amount = _to_decimal(row[amount_col])

        company_id = ctx.company_code_id.get(company_code)
        if company_id is None:
            continue

        line_item_id = ctx.line_item_id.get(line_item)

        rows.append(
            table_model(
                company_code_id=company_id,
                line_item_name=line_item,
                line_item_id=line_item_id,
                amount=amount,
            )
        )

    session.add_all(rows)
    session.commit()
    rows_loaded = len(rows)
    logger.debug("load_unsegmented_pnl_finished", rows=rows_loaded)
    logger.info("load_unsegmented_pnl_total", rows=rows_loaded)
    return rows_loaded
