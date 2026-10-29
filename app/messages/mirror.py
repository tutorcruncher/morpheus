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
3. Run reconcile_mirror_companies and check_mirror, wait for the mirror queue to drain, and repeat
   until check_mirror returns all zeros: both only queue copies, which step 4 would drop. Then
   setval the sequences.
4. In one config change, point DATABASE_URL at Heroku and unset MIRROR_DATABASE_URL.
5. Scale mirror to 0, worker and beat back up, maintenance off.

Delete this module once Heroku Postgres is the primary database.
"""

import logging
from itertools import batched

from sqlalchemy import Row, Table, TextClause, delete, event, make_url, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError, OperationalError
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
# minutes-long TCP connect timeout. A stalled one (a lock wait, a half-open connection after a
# failover) fails the statement into a retry rather than pinning the child. `options` replaces
# make_engine's, so the timezone is repeated.
MIRROR_CONNECT_ARGS = {
    'connect_timeout': 5,
    'options': '-c timezone=UTC -c statement_timeout=30s -c lock_timeout=5s',
    'keepalives_idle': 30,
    'keepalives_interval': 10,
    'keepalives_count': 3,
}

# check_mirror compares this many ids per query, an index range scan well inside the mirror's
# statement_timeout, and queues the rows that differ this many per copy.
CHECK_RANGE = 100_000
CHECK_COPY_SIZE = 1000
# The columns check_mirror compares. A row missing on either side shows in its id; the others are
# the ones that change after insert (message_groups, events and links never do). cost::numeric, as
# float8's text form differs between Postgres versions.
CHECKED_COLUMNS = {
    'companies': 'id, code',
    'message_groups': 'id',
    'messages': 'id, status, update_ts, cost::numeric',
    'events': 'id',
    'links': 'id',
}

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
            **MIRROR_CONNECT_ARGS,
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
    # Errors a later attempt gets past: the mirror down or restarting, a statement or lock timeout
    # (all OperationalError), or a race with another copy (IntegrityError). A DataError or
    # ProgrammingError, e.g. an enum value the mirror lacks, fails the same way every time.
    autoretry_for=(OperationalError, IntegrityError),
    # Waits double from 1s up to 10 minutes: 14 retries span about an hour.
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=False,
    max_retries=14,
    ignore_result=True,
)
def mirror_rows(table_name: str, ids: list[int]) -> None:
    """Make the mirror's copy of these rows match the primary: upsert the rows still on the primary
    and delete the ones gone from it.

    Retries ride out an hour-long mirror outage, or a race with another copy, e.g. a re-created company whose
    code the mirror still holds on the renamed row. An exhausted task fails into Sentry.
    """
    if not settings.mirror_database_url:
        # Unset while copies were still queued: nothing to copy to.
        return
    with get_mirror_engine().begin() as mirror_conn:
        _copy(mirror_conn, SQLModel.metadata.tables[table_name], ids)


@celery_app.task(name='app.messages.mirror.reconcile_mirror_companies', ignore_result=True)
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


def check_mirror() -> dict[str, int]:
    """Compare the mirror with the primary and queue a copy of every row that differs.

    Run with `heroku run` after the backfill, and again in the switch window while
    MIRROR_DATABASE_URL is still set. Each id range is compared by one md5 per side, then row by row
    where the two differ. Returns the rows queued per table: let the mirror queue drain and run it
    again until every count is 0.
    """
    queued = {}
    for table_name, columns in CHECKED_COLUMNS.items():
        queued[table_name] = 0
        row_md5 = f'md5(row({columns})::text)'
        in_range = f'FROM {table_name} WHERE id >= :lo AND id < :hi'
        range_md5 = text(f"SELECT md5(string_agg({row_md5}, '' ORDER BY id)) {in_range}")
        row_md5s = text(f'SELECT id, {row_md5} {in_range}')
        bounds = [v for rows in _on_both(text(f'SELECT min(id), max(id) FROM {table_name}')) for v in rows[0] if v]
        if not bounds:
            continue
        for lo in range(min(bounds), max(bounds) + 1, CHECK_RANGE):
            params = {'lo': lo, 'hi': lo + CHECK_RANGE}
            primary, mirror = _on_both(range_md5, params)
            if primary == mirror:
                continue
            primary, mirror = (dict(rows) for rows in _on_both(row_md5s, params))  # ty:ignore[no-matching-overload]
            ids = sorted(id_ for id_ in primary.keys() | mirror.keys() if primary.get(id_) != mirror.get(id_))
            logger.info('re-copying %d %s rows from id %d', len(ids), table_name, lo)
            for batch in batched(ids, CHECK_COPY_SIZE):
                mirror_ids(table_name, list(batch))
            queued[table_name] += len(ids)
    return queued


def _on_both(query: TextClause, params: dict | None = None) -> tuple[list[Row], list[Row]]:
    with get_session() as db:
        primary = list(db.execute(query, params))  # ty:ignore[deprecated]
    with get_mirror_engine().connect() as mirror_conn:
        mirror = list(mirror_conn.execute(query, params))
    return primary, mirror


def _copy(mirror_conn: Connection, table: Table, ids: list[int]) -> None:
    columns = [c for c in table.c if c.name not in SKIPPED_COLUMNS]
    # Read and released before the mirror is touched, so the primary never sits idle in a
    # transaction while the mirror answers.
    with get_session() as db:
        rows = db.execute(select(*columns).where(table.c.id.in_(ids))).mappings().all()  # ty:ignore[deprecated]
    # Parents first, so the upsert below never trips a foreign key. A parent is missing when it
    # predates the mirror, e.g. an open event on an email sent before MIRROR_DATABASE_URL was set.
    for fk in table.foreign_keys:
        parent_ids = {row[fk.parent.name] for row in rows}
        if not parent_ids:
            continue
        present = mirror_conn.execute(select(fk.column).where(fk.column.in_(parent_ids))).scalars().all()
        if missing := parent_ids.difference(present):
            _copy(mirror_conn, fk.column.table, sorted(missing))

    if table.name == 'events' and rows:
        _sync_message_state(mirror_conn, {row['message_id'] for row in rows})

    if gone := set(ids).difference(row['id'] for row in rows):
        if table.name == 'companies':
            # A purged company's CASCADE foreign keys take its whole history in this one statement,
            # which runs for minutes, as the primary's purge does. Only the mirror worker waits.
            mirror_conn.exec_driver_sql('SET LOCAL statement_timeout = 0')
        mirror_conn.execute(delete(table).where(table.c.id.in_(gone)))
    if rows:
        upsert = pg_insert(table).values([dict(row) for row in rows])
        mirror_conn.execute(
            upsert.on_conflict_do_update(
                index_elements=[table.c.id],
                set_={c.name: upsert.excluded[c.name] for c in columns if c.name != 'id'},
                # A message's update_ts only moves forward on the primary, so a copy read before one
                # that has already landed must not undo it. Equal still copies: a cost update leaves
                # update_ts as it is.
                where=table.c.update_ts <= upsert.excluded.update_ts if table.name == 'messages' else None,
            )
        )


def _sync_message_state(mirror_conn: Connection, message_ids: set[int]) -> None:
    """Set these messages' status, update_ts and cost on the mirror to the primary's.

    The update_message trigger moves a message's status and update_ts as its events land, and the
    mirror's events land in a different order to the primary's: a tie on ts goes to whichever lands
    first. Synced before the events are upserted, the message's update_ts is at least each event's
    ts, so the mirror's trigger leaves it alone. Only these columns, as a full copy would rewrite the
    body on every event.
    """
    messages = SQLModel.metadata.tables['messages']
    state = (messages.c.status, messages.c.update_ts, messages.c.cost)
    with get_session() as db:
        primary = db.execute(select(messages.c.id, *state).where(messages.c.id.in_(message_ids))).all()  # ty:ignore[deprecated]
    for id_, *values in primary:
        mirror_conn.execute(
            update(messages)
            .where(
                messages.c.id == id_,
                messages.c.update_ts <= values[1],
                or_(*(column.is_distinct_from(value) for column, value in zip(state, values))),
            )
            .values(dict(zip([c.name for c in state], values)))
        )
