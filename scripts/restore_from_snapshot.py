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
That equivalence rests on the pre-flight refusing to start when the target already holds any id or
group uuid we are about to restore, so nothing --rollback deletes can be a row we did not write.

The whole load is one transaction: if anything fails at any point, the database rolls all of it back
and the target is untouched. --rollback is for undoing a restore that already committed.

Needs pyarrow, which the app does not depend on -- run it with `uv run --with pyarrow python ...`.

Before running against production: take a manual RDS snapshot, and go off-peak -- every inserted
event fires the update_message AFTER INSERT trigger, so that step dominates the run time.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import io
import json
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras

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


def jsonl_files(input_dir: Path, table: str) -> list[Path]:
    return sorted((input_dir / table).rglob('*.jsonl.gz'))


class Column:
    """One column's values, with the .to_pylist() the loading code calls on it."""

    def __init__(self, values: list):
        self._values = values

    def to_pylist(self) -> list:
        return self._values


class Rows:
    """The part of the pyarrow Table interface this script uses, backed by plain dicts.

    Exists so the input can arrive as gzipped JSON Lines instead of parquet, which is what the
    remote loader downloads from S3: a dyno's slug holds only what pyproject.toml declares, and
    JSON Lines needs nothing but the standard library.

    JSON Lines rather than CSV because CSV cannot express null -- it writes an empty field for both
    None and '', and the snapshot has columns that are genuinely one or the other (external_id,
    subject, to_address). Collapsing the two would change the restored rows, and Route A exists to
    put them back exactly as they were.
    """

    def __init__(self, rows: list[dict], column_names: list[str] | None = None):
        self._rows = rows
        if column_names is None:
            column_names = []
            for r in rows:
                for k in r:
                    if k not in column_names:
                        column_names.append(k)
        self.column_names = column_names

    @property
    def num_rows(self) -> int:
        return len(self._rows)

    def __getitem__(self, column: str) -> Column:
        return Column([r.get(column) for r in self._rows])

    def select(self, columns: list[str]) -> 'Rows':
        return Rows([{c: r.get(c) for c in columns} for r in self._rows], list(columns))

    def to_pylist(self) -> list[dict]:
        return [{c: r.get(c) for c in self.column_names} for r in self._rows]


def read_jsonl(files: list[Path], columns: list[str] | None) -> Rows:
    rows = []
    for f in files:
        with gzip.open(f, 'rt', encoding='utf-8') as fh:
            for line in fh:
                if line.strip():
                    rows.append(json.loads(line))
    table = Rows(rows)
    return table.select(columns) if columns else table


def read_table(input_dir: Path, table: str, columns: list[str] | None = None):
    """One table's rows, from whichever format is on disk.

    JSON Lines is what the S3 path delivers; parquet is what the local extract produces. Both are
    read here so the rest of the script never learns which one it got.
    """
    if jsonl := jsonl_files(input_dir, table):
        return read_jsonl(jsonl, columns)

    # pyarrow is imported here rather than at module scope so the pure helpers below can be
    # imported and tested without it, and so the remote run never needs it at all.
    import pyarrow as pa
    import pyarrow.parquet as pq

    files = parquet_files(input_dir, table)
    if not files:
        sys.exit(f'no parquet or jsonl.gz files under {input_dir / table}')
    return pa.concat_tables([pq.read_table(f, columns=columns) for f in files])


def target_columns(cur, table: str) -> list[str]:
    cur.execute(
        'select column_name from information_schema.columns where table_schema=%s and table_name=%s',
        ('public', table),
    )
    return [r[0] for r in cur.fetchall()]


def resolve_remap(snap_by_id: dict[int, str], used_ids: list[int], current_by_code: dict[str, int]):
    """snapshot companies.id -> current companies.id, matched on code. Returns (mapping, problems).

    The subaccount delete removed the companies rows outright; later sends re-created them under the
    same codes with fresh ids. Codes are unique and stable, so they are the join key -- ids are not.
    Getting this wrong files one agency's mail under another, so it is resolved for every id the rows
    actually use, and any id that cannot be resolved is an error rather than a row left behind.
    """
    mapping, problems = {}, []
    for old_id in used_ids:
        code = snap_by_id.get(old_id)
        if code is None:
            problems.append(f'snapshot company id {old_id} has no row in the snapshot companies table')
        elif code not in current_by_code:
            problems.append(f'{code!r} (snapshot id {old_id}) does not exist in the target database')
        else:
            mapping[old_id] = current_by_code[code]
    return mapping, problems


def preflight_problems(existing_ids: dict[str, int], uuid_conflicts: list[tuple[str, int, int]]) -> list[str]:
    """Reasons the target is not in a fit state to restore into. Empty means go.

    Two separate hazards, both fatal before a row is written rather than partway through:

    `existing_ids` -- how many of the ids we are about to restore the target already holds. Should be
    zero: these ids were allocated before the delete and nothing since can have reused them. If it is
    not zero, either a previous restore already committed (roll that back first) or something is
    wrong enough to stop for. Refusing here is also what makes --rollback exact: every id in the
    parquet was then written by us, so deleting them all cannot touch a row we did not create.

    `uuid_conflicts` -- (uuid, id in target, id we would restore). message_groups.uuid carries its own
    unique index, which `ON CONFLICT (id)` does not arbitrate, so a group already present under a
    different id aborts the transaction on a bare duplicate-key error. Reachable whenever Route B has
    loaded the same agency: 26,758 of these two agencies' group uuids appear in both datasets. The
    documented order avoids it (Route B only supplies the post-snapshot tail, which shares none), but
    a clear refusal beats a cryptic abort after staging half a million rows.
    """
    problems = []
    for table, n in sorted(existing_ids.items()):
        if n:
            problems.append(f'{table}: {n:,} of the ids to restore already exist in the target')
    if uuid_conflicts:
        shown = ', '.join(f'{u} (target id {db}, restoring {mine})' for u, db, mine in uuid_conflicts[:3])
        problems.append(
            f'message_groups: {len(uuid_conflicts):,} uuid(s) already exist under a different id — {shown}'
            + ('…' if len(uuid_conflicts) > 3 else '')
        )
    return problems


def sequence_problems(max_ids: dict[str, int], sequences: dict[str, int]) -> list[str]:
    """Tables whose restored ids reach their sequence, which a future insert would then collide with.

    Restoring original primary keys is only safe while every one of them sits below the sequence:
    the ids were allocated before the delete and the sequence has only moved forward since, so this
    should always hold -- and if it does not, something is wrong enough to stop for.
    """
    return [f'{t}: max id {max_ids[t]:,} >= sequence {sequences[t]:,}' for t in max_ids if max_ids[t] >= sequences[t]]


def check_target_clean(cur, input_dir: Path) -> None:
    """Refuse before writing anything if the target already holds ids or uuids we are restoring."""
    existing = {}
    for table in TABLES:
        ids = read_table(input_dir, table, ['id'])['id'].to_pylist()
        cur.execute(f'select count(*) from {table} where id = any(%s)', (ids,))
        existing[table] = cur.fetchone()[0]

    groups = read_table(input_dir, 'message_groups', ['id', 'uuid'])
    mine = dict(zip(groups['uuid'].to_pylist(), groups['id'].to_pylist()))
    cur.execute('select uuid::text, id from message_groups where uuid::text = any(%s)', (list(mine),))
    conflicts = [(u, db_id, mine[u]) for u, db_id in cur.fetchall() if db_id != mine[u]]

    problems = preflight_problems(existing, conflicts)
    if problems:
        sys.exit(
            'the target is not clean for this restore:\n  ' + '\n  '.join(problems) + '\n\n'
            'If a previous restore committed, roll it back first. If Route B loaded these agencies, '
            'roll that batch back — Route A supersedes it, since these are the real messages rather '
            'than reconstructions. Nothing has been written.'
        )
    log('  target is clean: no restored id or group uuid already present')


def company_remap(cur, input_dir: Path) -> dict[int, int]:
    snap = read_table(input_dir.parent / 'route_a', 'companies') if (input_dir.parent / 'route_a').exists() else None
    if snap is None:
        sys.exit(f'need the snapshot companies table at {input_dir.parent / "route_a" / "companies"}')
    snap_by_id = dict(zip(snap['id'].to_pylist(), snap['code'].to_pylist()))

    # Every table that carries a company_id, not just message_groups: the remap is applied to all of
    # them, so building it from one would let an id the other uses fall through unmapped.
    used = set()
    for table in TABLES:
        if 'company_id' in read_table(input_dir, table).column_names:
            used |= set(read_table(input_dir, table, ['company_id'])['company_id'].to_pylist())
    used = sorted(used)

    cur.execute('select code, id from companies')
    mapping, problems = resolve_remap(snap_by_id, used, dict(cur.fetchall()))
    if problems:
        sys.exit(
            'cannot map every company:\n  ' + '\n  '.join(problems) + '\n\n'
            'Every code the restored rows belong to must already exist in the target. If one is '
            'missing, the agency has not sent anything since the wipe -- create the companies row '
            'first (a plain insert of the code) and re-run.'
        )
    return mapping


def check_sequences(cur, input_dir: Path, allow_overlap: bool) -> None:
    max_ids, sequences = {}, {}
    for table in TABLES:
        max_ids[table] = max(read_table(input_dir, table, ['id'])['id'].to_pylist())
        cur.execute('select last_value from %s' % f'{table}_id_seq')
        sequences[table] = cur.fetchone()[0]
        log(f'  {table:<15} max restored id {max_ids[table]:>12,}   sequence at {sequences[table]:>12,}')

    problems = sequence_problems(max_ids, sequences)
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
        if 'company_id' in row:
            # An id missing from the remap must stop the load, never pass through: the snapshot's
            # company ids mean nothing in the target, so keeping one files this agency's mail under
            # whichever company happens to hold that id now — silent, and invisible afterwards.
            if row['company_id'] not in remap:
                sys.exit(
                    f'{table}: company_id {row["company_id"]} has no mapping to a current company. '
                    'Nothing has been written. This means the snapshot companies table and the rows '
                    'disagree, which should not happen — do not bypass it.'
                )
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
    """Delete exactly the restored ids, children first.

    Deleting every id in the parquet is only the same thing as "undo the restore" because the load
    refuses to start when the target already holds any of them (check_target_clean). That guarantee
    is what makes this safe without a manifest: nothing here can remove a row the restore did not
    write. If that pre-flight is ever bypassed, this is no longer true and could delete live rows.
    """
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

    log('pre-flight')
    check_sequences(cur, args.input, args.allow_id_overlap)
    check_target_clean(cur, args.input)

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
