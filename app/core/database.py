from pathlib import Path
from typing import TypeVar

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session, SQLModel, select

from app.core.config import settings

T = TypeVar('T', bound=SQLModel)

BOOTSTRAP_SQL_PATH = Path(__file__).parent / 'bootstrap.sql'
POST_BOOTSTRAP_SQL_PATH = Path(__file__).parent / 'post_bootstrap.sql'

# The bootstrap DDL takes ACCESS EXCLUSIVE locks on messages/events. Scope a short lock_timeout to
# these connections only (NOT the app engine, whose queries legitimately wait for locks) so a
# conflicting lock holder makes bootstrap fail loudly with a traceback rather than hang for the full
# boot deadline and dam the lock queue behind a waiting exclusive request (issue #511).
BOOTSTRAP_LOCK_TIMEOUT = '5s'


class DBSession(Session):
    """SQLModel session with Django-like helpers (matches tc-ai-backend's DBSession)."""

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
        except IntegrityError:  # pragma: no cover  -- race-condition fallback
            self.rollback()
            instance = self.exec(stmt).one()
            return instance, False


def _make_engine(pool_size: int, max_overflow: int):
    return create_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=settings.db_pool_timeout,
        connect_args={'options': '-c timezone=UTC'},
    )


engine = _make_engine(settings.db_pool_size, settings.db_max_overflow)
SessionLocal = sessionmaker(class_=DBSession, autocommit=False, autoflush=False, bind=engine)
SessionCls = SessionLocal


def configure_worker_engine() -> None:
    """Rebuild the engine with the small worker pool. Called post-fork from a celery prefork
    child's worker_process_init.

    Each child processes one task at a time (worker_prefetch_multiplier=1) via a single
    get_session(), so it needs only a couple of connections. Leaving it on the web pool (20+10)
    multiplied per child × per worker dyno and, next to both blue-green web colours, threatened
    RDS max_connections during cutover (MORPHEUS-3DNG). Rebuilding here also drops the pool the
    child inherited across fork instead of sharing the parent's connections.
    """
    global engine, SessionLocal, SessionCls
    engine.dispose()
    engine = _make_engine(settings.db_worker_pool_size, settings.db_worker_max_overflow)
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
            cur.execute(f"SET lock_timeout = '{BOOTSTRAP_LOCK_TIMEOUT}'")
            cur.execute('CREATE EXTENSION IF NOT EXISTS btree_gin')
            cur.execute(BOOTSTRAP_SQL_PATH.read_text())
        raw_conn.commit()
    finally:
        raw_conn.close()

    # Import models so they're registered with SQLModel.metadata before create_all.
    from app.messages import models  # noqa: F401

    with engine.connect() as conn:
        conn.exec_driver_sql(f"SET lock_timeout = '{BOOTSTRAP_LOCK_TIMEOUT}'")
        SQLModel.metadata.create_all(conn)
        conn.commit()

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(f"SET lock_timeout = '{BOOTSTRAP_LOCK_TIMEOUT}'")
            cur.execute(POST_BOOTSTRAP_SQL_PATH.read_text())
        raw_conn.commit()
    finally:
        raw_conn.close()
