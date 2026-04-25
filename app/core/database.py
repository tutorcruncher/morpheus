from pathlib import Path
from typing import TypeVar

from sqlalchemy import create_engine, exists
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session, SQLModel, select
from sqlmodel.sql.expression import SelectOfScalar

from app.common.api.errors import HTTP404
from app.core.config import settings

T = TypeVar('T', bound=SQLModel)

BOOTSTRAP_SQL_PATH = Path(__file__).parent / 'bootstrap.sql'
POST_BOOTSTRAP_SQL_PATH = Path(__file__).parent / 'post_bootstrap.sql'


class DBSession(Session):
    """SQLModel session with Django-like helpers."""

    def create(self, instance: T) -> T:
        self.add(instance)
        self.commit()
        self.refresh(instance)
        return instance

    def exists(self, model: type[T], **kwargs) -> bool:
        wheres = [getattr(model, key) == value for key, value in kwargs.items()]
        return bool(self.exec(select(exists().where(*wheres))).one())

    def get_or_404(self, query_or_model, **kwargs) -> T:
        if isinstance(query_or_model, SelectOfScalar):
            instance = self.exec(query_or_model.filter_by(**kwargs)).one_or_none()
            model_name = query_or_model.column_descriptions[0]['name']
        else:
            instance = self.exec(select(query_or_model).filter_by(**kwargs)).one_or_none()
            model_name = query_or_model.__name__
        if not instance:
            raise HTTP404(f'{model_name} not found')
        return instance

    def get_or_create(self, model: type[T], defaults: dict | None = None, **kwargs) -> tuple[T, bool]:
        stmt = select(model)
        for key, value in kwargs.items():
            stmt = stmt.where(getattr(model, key) == value)

        instance = self.exec(stmt).one_or_none()
        if instance:
            return instance, False

        create_kwargs = {**kwargs, **(defaults or {})}
        instance = model(**create_kwargs)

        try:
            self.add(instance)
            self.commit()
            self.refresh(instance)
            return instance, True
        except IntegrityError:
            self.rollback()
            instance = self.exec(stmt).one()
            return instance, False


engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    connect_args={'options': '-c timezone=UTC'},
)
SessionLocal = sessionmaker(class_=DBSession, autocommit=False, autoflush=False, bind=engine)
SessionCls = SessionLocal


def get_session() -> DBSession:
    return SessionCls()


def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()


def create_db_and_tables() -> None:
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute('CREATE EXTENSION IF NOT EXISTS btree_gin')
            cur.execute(BOOTSTRAP_SQL_PATH.read_text())
        raw_conn.commit()
    finally:
        raw_conn.close()

    # Import models so they're registered with SQLModel.metadata before create_all.
    from app.messages import models  # noqa: F401

    SQLModel.metadata.create_all(engine)

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(POST_BOOTSTRAP_SQL_PATH.read_text())
        raw_conn.commit()
    finally:
        raw_conn.close()
