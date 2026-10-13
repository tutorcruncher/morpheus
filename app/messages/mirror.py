"""Copy every write to a second database while Morpheus moves from RDS to Heroku Postgres (issue #502).

With MIRROR_DATABASE_URL set, every ORM insert, update and delete on the primary database queues
mirror_rows once its transaction commits. The task copies the rows' current primary state, ids
included, so the message ids TC2 holds stay valid once the mirror becomes the primary. Unset, the
listeners return straight away and nothing is queued.

The tasks run on their own `mirror` queue (app/core/celery.py task_routes), served by the Procfile's
mirror process, so a slow or unreachable mirror never delays sends. Scale that process up before
setting MIRROR_DATABASE_URL: until it runs, copies wait in Redis.

Bulk statements (db.execute(update(...)) / delete(...)) skip the ORM events, so their callers queue
mirror_ids themselves. delete_old_emails is not mirrored: it is off, and #502 switches it on against
Heroku once Heroku is the primary.

mirror_rows treats DATABASE_URL as the truth: it deletes mirror rows the primary lacks and
overwrites the rest. So the switch goes in this order, or a process still on the old settings copies
RDS over the new primary:
1. Stop writes to RDS: maintenance on, worker and beat scaled to 0 (mirror keeps running).
2. Wait for the mirror queue to drain.
3. Run reconcile_mirror_companies and the content check, then setval the sequences.
4. In one config change, point DATABASE_URL at Heroku and unset MIRROR_DATABASE_URL.
5. Scale mirror to 0, worker and beat back up, maintenance off.

Delete this module once Heroku Postgres is the primary database.
"""

import logging

from sqlalchemy import Table, delete, event, make_url, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Mapper, SessionTransaction, object_session
from sqlmodel import SQLModel

from app.core.celery import celery_app
from app.core.config import settings
from app.core.database import DBSession, get_session, make_engine
from app.messages.models import Company, Event, Link, Message, MessageGroup

logger = logging.getLogger('worker.mirror')

MIRRORED_MODELS = (Company, MessageGroup, Message, Event, Link)
# Session.info keys holding {table name: ids}: written in the open transaction, then committed and
# waiting for the session to hand its connection back before they are queued.
PENDING_KEY = 'mirror_pending'
READY_KEY = 'mirror_ready'
# The mirror's create_tsvector trigger fills messages.vector on insert, so it is never copied.
SKIPPED_COLUMNS = {'vector'}
# An unreachable mirror must cost a mirror worker child seconds per attempt, not the OS's
# minutes-long TCP connect timeout.
CONNECT_TIMEOUT = 5

_mirror_engine: Engine | None = None


def get_mirror_engine() -> Engine:
    """Built on first use, which is inside a worker child, so no pool is shared across the prefork."""
    global _mirror_engine
    if _mirror_engine is None:
        assert settings.mirror_database_url
        # TLS unless the URL picks its own sslmode, as the local and CI test databases do.
        tls = {} if 'sslmode' in make_url(settings.mirror_database_url).query else {'sslmode': 'require'}
        _mirror_engine = make_engine(
            settings.mirror_database_url,
            settings.db_worker_pool_size,
            settings.db_worker_max_overflow,
            # A failed copy's error, logged and retried, then names only the SQL, not the row values.
            hide_parameters=True,
            connect_timeout=CONNECT_TIMEOUT,
            **tls,
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


def _mark_committed(session: DBSession) -> None:
    ready = session.info.setdefault(READY_KEY, {})
    for table_name, ids in session.info.pop(PENDING_KEY, {}).items():
        ready.setdefault(table_name, set()).update(ids)


def _queue_ready(session: DBSession, transaction: SessionTransaction) -> None:
    # after_commit fires while the session still holds its pooled connection, so queueing there
    # would hold the connection across a broker round trip (MORPHEUS-3DNG). The root transaction's
    # after_transaction_end fires once the connection has gone back to the pool.
    if transaction.parent is None:
        for table_name, ids in session.info.pop(READY_KEY, {}).items():
            mirror_ids(table_name, sorted(ids))


def _drop_pending(session: DBSession) -> None:
    session.info.pop(PENDING_KEY, None)
    session.info.pop(READY_KEY, None)


for _model in MIRRORED_MODELS:
    for _event_name in ('after_insert', 'after_update', 'after_delete'):
        event.listen(_model, _event_name, _record_write)
event.listen(DBSession, 'after_commit', _mark_committed)
event.listen(DBSession, 'after_transaction_end', _queue_ready)
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


@celery_app.task(name='app.messages.mirror.reconcile_mirror_companies')
def reconcile_mirror_companies() -> int:
    """Re-copy every company on the mirror that no longer matches the primary.

    A purge or rename whose copy never landed (lost to a broker blip, or out of retries) would
    otherwise leave the old row on the mirror, and through its CASCADE foreign keys the company's
    whole history. Companies are a row per agency or branch, so comparing them all is cheap.
    """
    if not settings.mirror_database_url:
        return 0
    table = SQLModel.metadata.tables['companies']
    query = select(table.c.id, table.c.code)
    with get_session() as db:
        primary = dict(db.execute(query).tuples().all())  # ty:ignore[deprecated]
    with get_mirror_engine().connect() as mirror_conn:
        mirror = dict(mirror_conn.execute(query).tuples().all())
    out_of_step = sorted(id_ for id_, code in mirror.items() if primary.get(id_) != code)
    if out_of_step:
        logger.info('re-copying mirror companies out of step with the primary: %s', out_of_step)
        mirror_ids('companies', out_of_step)
    return len(out_of_step)


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
