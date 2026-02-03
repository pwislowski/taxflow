from sqlmodel import SQLModel, select

from app.config import app_config
from app.db.models import CompanyCode
from app.db.session import get_engine, session_scope


def test_get_engine_uses_config_path_and_echo_true(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setattr(app_config, "database_path", db_path)
    monkeypatch.setattr(app_config, "db_echo", True)

    engine = get_engine()

    assert engine.echo is True
    assert engine.url.database == str(db_path)
    assert str(engine.url).startswith("sqlite:///")


def test_session_scope_roundtrip(tmp_path, monkeypatch):
    db_path = tmp_path / "roundtrip.db"
    monkeypatch.setattr(app_config, "database_path", db_path)
    monkeypatch.setattr(app_config, "db_echo", True)

    engine = get_engine()
    SQLModel.metadata.create_all(engine)

    with session_scope() as session:
        session.add(CompanyCode(id=1, code="AB01"))
        session.commit()

    with session_scope() as session:
        results = session.exec(select(CompanyCode)).all()
        assert [(row.id, row.code) for row in results] == [(1, "AB01")]
