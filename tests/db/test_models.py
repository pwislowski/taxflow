from sqlalchemy import inspect
from sqlmodel import SQLModel

from app.config import app_config
from app.db.session import get_engine


def _expected_columns():
    return {
        "companycode": {"id", "code"},
        "lineitem": {"id", "name"},
        "rndservice": {
            "id",
            "company_code_id",
            "line_item_id",
            "amount",
            "created_on",
        },
        "sharedservicestotalcharge": {
            "id",
            "company_code_id",
            "line_item_id",
            "amount",
            "created_on",
        },
        "grandtotal": {
            "id",
            "company_code_id",
            "line_item_id",
            "amount",
            "created_on",
        },
        "intellectualpropertyservice": {
            "id",
            "company_code_id",
            "line_item_id",
            "account_number",
            "amount",
        },
        "productbusiness": {
            "id",
            "company_code_id",
            "line_item_name",
            "line_item_id",
            "amount",
        },
        "productbusinesssegmented": {
            "id",
            "company_code_id",
            "line_item_name",
            "line_item_id",
            "category",
            "amount",
        },
    }


def test_models_create_expected_tables_and_columns(tmp_path, monkeypatch):
    db_path = tmp_path / "models.db"
    monkeypatch.setattr(app_config, "database_path", db_path)
    monkeypatch.setattr(app_config, "db_echo", False)

    engine = get_engine()
    SQLModel.metadata.create_all(engine)

    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())

    expected_tables = set(_expected_columns().keys())
    assert expected_tables.issubset(table_names)

    for table_name, expected_cols in _expected_columns().items():
        columns = {col["name"] for col in inspector.get_columns(table_name)}
        assert expected_cols == columns
