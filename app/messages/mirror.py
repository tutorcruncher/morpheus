"""Copy every write to a second database while Morpheus moves from RDS to Heroku Postgres (issue #502).

With MIRROR_DATABASE_URL set, every ORM insert, update and delete on the primary database queues
mirror_rows once its transaction commits. The task copies the rows' current primary state, ids
included, so the message ids TC2 holds stay valid once the mirror becomes the primary. Unset, the
listeners return straight away and nothing is queued.

Bulk statements (db.execute(update(...)) / delete(...)) skip the ORM events, so their callers queue
mirror_ids themselves. delete_old_emails is not mirrored: it is off, and #502 switches it on against
Heroku once Heroku is the primary.

Delete this module once Heroku Postgres is the primary database.
"""

import logging

from sqlalchemy import Table, delete, event, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Mapper, object_session
from sqlmodel import SQLModel

from app.core.celery import celery_app
from app.core.config import settings
from app.core.database import DBSession, get_session, make_engine
from app.messages.models import Company, Event, Link, Message, MessageGroup

logger = logging.getLogger('worker.mirror')

MIRRORED_MODELS = (Company, MessageGroup, Message, Event, Link)
# Session.info key holding {table name: ids} written in the open transaction, queued on commit.
PENDING_KEY = 'mirror_pending'
# The mirror's create_tsvector trigger fills messages.vector on insert, so it is never copied.
SKIPPED_COLUMNS = {'vector'}
# mirror_rows shares the worker with sends: an unreachable mirror must cost a worker child seconds
# per attempt, not the OS's minutes-long TCP connect timeout.
CONNECT_TIMEOUT = 5

_mirror_engine: Engine | None = None


def get_mirror_engine() -> Engine:
    """Built on first use, which is inside a worker child, so no pool is shared across the prefork."""
    global _mirror_engine
    if _mirror_engine is None:
        assert settings.mirror_database_url
        _mirror_engine = make_engine(
            settings.mirror_database_url,
            settings.db_worker_pool_size,
            settings.db_worker_max_overflow,
            connect_timeout=CONNECT_TIMEOUT,
        )
    return _mirror_engine


def mirror_ids(table_name: str, ids: list[int]) -> None:
    """Queue a copy of these rows to the mirror database; a no-op unless MIRROR_DATABASE_URL is set."""
    if not settings.mirror_database_url or not ids:
        return
    try:
        mirror_rows.delay(table_name, ids)
    except Exception:
        # The primary write has already committed: a broker blip must not turn it into an error.
        logger.exception('failed to queue mirror of %s ids=%s', table_name, ids)


def _record_write(mapper: Mapper, connection: Connection, target: SQLModel) -> None:
    if not settings.mirror_database_url:
        return
    session = object_session(target)
    assert session is not None
    pending = session.info.setdefault(PENDING_KEY, {})
    pending.setdefault(mapper.local_table.name, set()).add(target.id)  # ty:ignore[unresolved-attribute]


def _queue_pending(session: DBSession) -> None:
    for table_name, ids in session.info.pop(PENDING_KEY, {}).items():
        mirror_ids(table_name, sorted(ids))


def _drop_pending(session: DBSession) -> None:
    session.info.pop(PENDING_KEY, None)


for _model in MIRRORED_MODELS:
    for _event_name in ('after_insert', 'after_update', 'after_delete'):
        event.listen(_model, _event_name, _record_write)
event.listen(DBSession, 'after_commit', _queue_pending)
event.listen(DBSession, 'after_rollback', _drop_pending)


@celery_app.task(
    name='app.messages.mirror.mirror_rows',
    autoretry_for=(SQLAlchemyError,),
    retry_backoff=True,
    max_retries=5,
)
def mirror_rows(table_name: str, ids: list[int]) -> None:
    """Make the mirror's copy of these rows match the primary: upsert the rows still on the primary
    and delete the ones gone from it.

    Retries ride out a mirror blip, or a race with another copy, e.g. a re-created company whose
    code the mirror still holds on the renamed row. An exhausted task fails into Sentry.
    """
    if not settings.mirror_database_url:
        # Unset while copies were still queued: nothing to copy to.
        return
    with get_session() as db, get_mirror_engine().begin() as mirror_conn:
        _copy(db, mirror_conn, SQLModel.metadata.tables[table_name], ids)


def _copy(db: DBSession, mirror_conn: Connection, table: Table, ids: list[int]) -> None:
    columns = [c for c in table.c if c.name not in SKIPPED_COLUMNS]
    rows = db.execute(select(*columns).where(table.c.id.in_(ids))).mappings().all()  # ty:ignore[deprecated]
    # Parents first, so the upsert below never trips a foreign key. A parent is missing when it
    # predates the mirror, e.g. an open event on an email sent before MIRROR_DATABASE_URL was set.
    for fk in table.foreign_keys:
        parent_ids = {row[fk.parent.name] for row in rows}
        if not parent_ids:
            continue
        present = mirror_conn.execute(select(fk.column).where(fk.column.in_(parent_ids))).scalars().all()
        if missing := parent_ids.difference(present):
            _copy(db, mirror_conn, fk.column.table, sorted(missing))

    found = [row['id'] for row in rows]
    mirror_conn.execute(delete(table).where(table.c.id.in_(ids), table.c.id.not_in(found)))
    if rows:
        upsert = pg_insert(table).values([dict(row) for row in rows])
        mirror_conn.execute(
            upsert.on_conflict_do_update(
                index_elements=[table.c.id],
                set_={c.name: upsert.excluded[c.name] for c in columns if c.name != 'id'},
            )
        )
