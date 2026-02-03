from contextlib import contextmanager

from sqlmodel import Session, SQLModel, create_engine

from app.config import app_config


def get_engine():
    return create_engine(
        f"sqlite:///{app_config.database_path}",
        echo=app_config.db_echo,
    )


def create_db_and_tables():
    engine = get_engine()
    SQLModel.metadata.create_all(engine)


@contextmanager
def session_scope():
    engine = get_engine()
    with Session(engine) as session:
        yield session
