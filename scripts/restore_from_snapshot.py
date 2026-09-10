#!/usr/bin/env python3
"""
Restore deleted email history from an RDS snapshot export.

Route A of the issue #548 recovery, for the subaccounts whose deletion happened after the nightly
snapshot was taken. Unlike Route B (scripts/load_recovered.py), which loads rows *reconstructed* from
send logs, this loads the real rows straight out of a snapshot export: real bodies, real delivery
events, real click links, real provider ids. Nothing is rendered or guessed.

    uv run python scripts/restore_from_snapshot.py --dsn "$DSN" --input /path/to/route_a_filtered --dry-run
    uv run python scripts/restore_from_snapshot.py --dsn "$DSN" --input /path/to/route_a_filtered
    uv run python scripts/restore_from_snapshot.py --dsn "$DSN" --input /path/to/route_a_filtered --rollback

Input: route_a_filtered/{message_groups,messages,events,links}/**/*.parquet, produced by the
recovery folder's route_a_extract.py.

What it does, in dependency order (groups -> messages -> events -> links):
  1. Original primary keys are preserved. Deleted ids were never reused and the sequences have only
     moved forward, so message_groups.id, messages.id, events.id and links.id all go back exactly as
     they were -- which means events.message_id and links.message_id need no translation at all.
     The load refuses to start if any id is >= its sequence, since that would collide with a future
     insert (--allow-id-overlap to override, only if you know why).
  2. company_id is the ONE thing remapped. The delete removed the companies rows and later sends
     re-created them with new ids, so snapshot company ids are matched to current ones by `code`.
  3. Columns are the intersection of what the parquet has and what the target table has, minus the
     ones the database owns. The snapshot carries spam_status/spam_reason, which this codebase's
     models do not define, so whether they are restored depends on the target -- not on a guess here.
  4. `vector` is never inserted: the create_tsvector BEFORE INSERT trigger rebuilds it, exactly as it
     does for a normal send.
  5. COPY into a temp staging table, then INSERT ... SELECT in --batch-size chunks with
     ON CONFLICT (id) DO NOTHING, so a re-run is a no-op rather than a duplicate.

Rollback needs no bookkeeping: the ids are in the parquet files, so --rollback deletes exactly those
ids. Restored rows are therefore byte-identical to the originals, carrying no marker of the restore.

Before running against production: take a manual RDS snapshot, and go off-peak -- every inserted
event fires the update_message AFTER INSERT trigger, so that step dominates the run time.
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq

# Load order matters: each table's foreign keys point at the one before it.
TABLES = ['message_groups', 'messages', 'events', 'links']

# Columns the database fills in itself and we must never supply.
#   vector  -- rebuilt by the create_tsvector trigger on insert
SKIP_COLUMNS = {'vector'}

# Postgres array columns arrive from the parquet export as array literals ('{a,b,c}'), so the
# staging table holds them as text and the insert casts them back.
ARRAY_COLUMNS = {'tags', 'attachments'}
JSON_COLUMNS = {'extra'}


def log(msg: str) -> None:
    print(f'[{dt.datetime.now():%H:%M:%S}] {msg}', flush=True)


def parquet_files(input_dir: Path, table: str) -> list[Path]:
    return sorted((input_dir / table).rglob('*.parquet'))


def read_table(input_dir: Path, table: str, columns: list[str] | None = None):
    files = parquet_files(input_dir, table)
    if not files:
        sys.exit(f'no parquet files under {input_dir / table}')
    import pyarrow as pa

    return pa.concat_tables([pq.read_table(f, columns=columns) for f in files])


def target_columns(cur, table: str) -> list[str]:
    cur.execute(
        'select column_name from information_schema.columns where table_schema=%s and table_name=%s',
        ('public', table),
    )
    return [r[0] for r in cur.fetchall()]


def company_remap(cur, input_dir: Path) -> dict[int, int]:
    """snapshot companies.id -> current companies.id, matched on code.

    The subaccount delete removed the companies rows outright; later sends re-created them under the
    same codes with fresh ids. Codes are unique and stable, so they are the join key -- ids are not.
    """
    snap = read_table(input_dir.parent / 'route_a', 'companies') if (input_dir.parent / 'route_a').exists() else None
    if snap is None:
        sys.exit(f'need the snapshot companies table at {input_dir.parent / "route_a" / "companies"}')
    snap_by_id = dict(zip(snap['id'].to_pylist(), snap['code'].to_pylist()))

    groups = read_table(input_dir, 'message_groups', ['company_id'])
    used = sorted(set(groups['company_id'].to_pylist()))

    cur.execute('select code, id from companies')
    current = dict(cur.fetchall())

    mapping, missing = {}, []
    for old_id in used:
        code = snap_by_id.get(old_id)
        if code is None:
            missing.append(f'snapshot company id {old_id} has no row in the snapshot companies table')
        elif code not in current:
            missing.append(f'{code!r} (snapshot id {old_id}) does not exist in the target database')
        else:
            mapping[old_id] = current[code]
    if missing:
        sys.exit(
            'cannot map every company:\n  ' + '\n  '.join(missing) + '\n\n'
            'Every code the restored rows belong to must already exist in the target. If one is '
            'missing, the agency has not sent anything since the wipe -- create the companies row '
            'first (a plain insert of the code) and re-run.'
        )
    return mapping


def check_sequences(cur, input_dir: Path, allow_overlap: bool) -> None:
    """Every restored id must sit below its sequence, or a future insert collides with it."""
    problems = []
    for table in TABLES:
        ids = read_table(input_dir, table, ['id'])['id']
        max_id = max(ids.to_pylist())
        cur.execute('select last_value from %s' % f'{table}_id_seq')
        seq = cur.fetchone()[0]
        log(f'  {table:<15} max restored id {max_id:>12,}   sequence at {seq:>12,}')
        if max_id >= seq:
            problems.append(f'{table}: max id {max_id:,} >= sequence {seq:,}')
    if problems and not allow_overlap:
        sys.exit(
            'restored ids reach past the sequence:\n  ' + '\n  '.join(problems) + '\n\n'
            'Inserting them would collide with rows the database is about to create. '
            'Pass --allow-id-overlap only if you have bumped the sequences yourself.'
        )


def load_one(cur, input_dir: Path, table: str, remap: dict[int, int], batch_size: int) -> tuple[int, int]:
    """COPY one table into staging, then insert the rows that are not already there."""
    available = set(target_columns(cur, table))
    data = read_table(input_dir, table)
    cols = [c for c in data.column_names if c in available and c not in SKIP_COLUMNS]
    dropped = [c for c in data.column_names if c not in cols]
    if dropped:
        log(f'  {table}: not inserting {", ".join(sorted(dropped))}')

    staging = f'_restore_{table}'
    cur.execute(f'create temp table {staging} (like {table} including defaults excluding constraints) on commit drop')
    # The array/json columns are text in the parquet; widen them in staging and cast on insert.
    for col in cols:
        if col in ARRAY_COLUMNS | JSON_COLUMNS:
            cur.execute(f'alter table {staging} alter column {col} type text using {col}::text')
    cur.execute(f'alter table {staging} drop column if exists vector')

    buf = io.StringIO()
    rows = data.select(cols).to_pylist()
    remapped = 0
    for row in rows:
        if 'company_id' in row and row['company_id'] in remap:
            row['company_id'] = remap[row['company_id']]
            remapped += 1
        buf.write('\t'.join(pg_text(row[c]) for c in cols) + '\n')
    buf.seek(0)
    cur.copy_expert(f'copy {staging} ({", ".join(cols)}) from stdin', buf)
    log(f'  {table}: staged {len(rows):,} rows' + (f', {remapped:,} company ids remapped' if remapped else ''))

    select_cols = ', '.join(
        f'{c}::varchar[]' if c in ARRAY_COLUMNS else f'{c}::jsonb' if c in JSON_COLUMNS else c for c in cols
    )
    inserted = 0
    while True:
        cur.execute(
            f"""with batch as (delete from {staging} where id in
                    (select id from {staging} limit {batch_size}) returning *)
                insert into {table} ({', '.join(cols)})
                select {select_cols} from batch
                on conflict (id) do nothing"""
        )
        if not cur.rowcount:
            cur.execute(f'select count(*) from {staging}')
            if not cur.fetchone()[0]:
                break
            continue
        inserted += cur.rowcount
    return len(rows), inserted


def pg_text(v) -> str:
    """One value as a COPY-format text field."""
    if v is None:
        return '\\N'
    if v is True:
        return 't'
    if v is False:
        return 'f'
    s = str(v)
    return s.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')


def rollback(cur, input_dir: Path, batch_size: int) -> None:
    """Delete exactly the restored ids, children first."""
    for table in reversed(TABLES):
        ids = read_table(input_dir, table, ['id'])['id'].to_pylist()
        deleted = 0
        for i in range(0, len(ids), batch_size):
            cur.execute(f'delete from {table} where id = any(%s)', (ids[i : i + batch_size],))
            deleted += cur.rowcount
        log(f'  {table}: deleted {deleted:,} of {len(ids):,}')


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--dsn', required=True)
    ap.add_argument('--input', type=Path, required=True, help='route_a_filtered folder')
    ap.add_argument('--batch-size', type=int, default=5000)
    ap.add_argument('--dry-run', action='store_true', help='do everything, then roll the transaction back')
    ap.add_argument('--rollback', action='store_true', help='delete the restored ids and exit')
    ap.add_argument('--allow-id-overlap', action='store_true')
    args = ap.parse_args()

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = False
    cur = conn.cursor()

    if args.rollback:
        log('rollback: deleting restored ids')
        rollback(cur, args.input, args.batch_size)
        conn.commit()
        log('rollback committed')
        return

    log('checking sequences')
    check_sequences(cur, args.input, args.allow_id_overlap)

    remap = company_remap(cur, args.input)
    log(f'company remap: {", ".join(f"{o}->{n}" for o, n in sorted(remap.items()))}')

    totals = []
    for table in TABLES:
        read, inserted = load_one(cur, args.input, table, remap, args.batch_size)
        totals.append((table, read, inserted))
        log(f'  {table}: inserted {inserted:,} (skipped {read - inserted:,} already present)')

    log('')
    for table, read, inserted in totals:
        log(f'{table:<15} read {read:>9,}   inserted {inserted:>9,}')

    if args.dry_run:
        conn.rollback()
        log('\nDRY RUN — transaction rolled back, nothing written')
    else:
        conn.commit()
        log('\ncommitted')


if __name__ == '__main__':
    main()
