from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlmodel import Field, SQLModel


class CompanyCode(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None)
    code: str = Field(index=True)


class LineItem(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None, index=True)
    name: str


class RndService(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None, index=True)

    company_code_id: int = Field(
        foreign_key="companycode.id", index=True, nullable=False
    )
    line_item_id: int = Field(foreign_key="lineitem.id", index=True)
    amount: Decimal = Field(default=0, max_digits=5, decimal_places=3)
    created_on: Optional[datetime] = Field(index=True, default_factory=datetime.now)


class SharedServicesTotalCharge(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None, index=True)

    company_code_id: int = Field(
        foreign_key="companycode.id", index=True, nullable=False
    )
    line_item_id: int = Field(foreign_key="lineitem.id", index=True, nullable=False)
    amount: Decimal = Field(default=0, max_digits=5, decimal_places=3)
    created_on: Optional[datetime] = Field(index=True, default_factory=datetime.now)


class GrandTotal(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None, index=True)

    company_code_id: int = Field(
        foreign_key="companycode.id", index=True, nullable=False
    )
    line_item_id: int = Field(foreign_key="lineitem.id", index=True, nullable=False)
    amount: Decimal = Field(default=0, max_digits=5, decimal_places=3)
    created_on: Optional[datetime] = Field(index=True, default_factory=datetime.now)


class IntellectualPropertyService(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None, index=True)

    company_code_id: int = Field(
        foreign_key="companycode.id", index=True, nullable=False
    )
    line_item_id: int = Field(foreign_key="lineitem.id", index=True, nullable=False)
    account_number: int = Field(index=True)
    amount: Decimal = Field(default=0, max_digits=5, decimal_places=3)


class ProductBusiness(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None, index=True)

    company_code_id: int = Field(
        foreign_key="companycode.id", index=True, nullable=False
    )
    line_item_name: str = Field(index=True)
    line_item_id: Optional[int] = Field(
        default=None, foreign_key="lineitem.id", index=True
    )
    amount: Decimal = Field(default=0, max_digits=5, decimal_places=3)


class ProductBusinessSegmented(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None, index=True)

    company_code_id: int = Field(
        foreign_key="companycode.id", index=True, nullable=False
    )
    line_item_name: str = Field(index=True)
    line_item_id: Optional[int] = Field(
        default=None, foreign_key="lineitem.id", index=True
    )
    category: Optional[str] = None
    amount: Decimal = Field(default=0, max_digits=5, decimal_places=3)
