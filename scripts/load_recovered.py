#!/usr/bin/env python3
"""
Load recovered email records (rendered by the recovery pipeline) into a Morpheus database.

    uv run python scripts/load_recovered.py --dsn "$DATABASE_URL" --input "/path/to/rendered" \
        --agency <agency-code> --batch-id <agency-code>-2026-09-08 --dry-run
    uv run python scripts/load_recovered.py --dsn … --input … --agency … --batch-id … [--start 2025-09-01 --end 2026-08-27]
    uv run python scripts/load_recovered.py --dsn … --rollback <batch-id>

Input: rendered/<date>.csv.gz files (one row per email: send_ts, company_code, branch_id, group_uuid,
to_address, first_name, last_name, user_id, role_type, trigger, tags, subject, body, from_address,
reply_to, sources, context_keys).

What it does, one agency at a time:
  0. input: rows for the same group uuid + address sent within --dedupe-window seconds (default 5)
     are collapsed to one before anything is written. Merging papertrail with bigquery left ~10%
     exact duplicates and a further 6,930 pairs a few milliseconds apart, which an exact key missed.
  1. companies: ensures a row per "<agency>:<branch>" code (the codes TC2 sends with).
  2. message_groups: one per group uuid (rows without a uuid get a deterministic uuid5 per code+minute),
     created_ts = earliest send in the group, message_method 'email-mandrill', from_email from the style.
     Groups whose uuid already exists are reused, never modified. 54 uuids are carried by two different
     agencies, so ownership is settled first — from the whole input, then overlaid with what the database
     already holds, which wins — and a row whose uuid belongs to someone else takes the uuid5 path and
     gets its own group. Without that, those rows block 8 of the 25 agency loads outright.
  3. messages: COPY into a TEMP staging table, then INSERT … SELECT in batches of --batch-size, skipping
     rows the database already holds for the same group uuid + address within --dedupe-window seconds.
     The window matters: every agency kept sending after its own subaccount was deleted, so the rendered
     files carry post-deletion sends that are still live in production, and their send_ts comes from a
     log line rather than the app — an exact match never fires and re-inserts them. Every row is stamped
     extra.recovered_batch = <batch-id> (plus recovered_from, trigger, sources) so a batch can be removed
     again with --rollback. The BEFORE INSERT trigger fills the search vector as for normal sends.
     No schema changes are made: the script only inserts into companies, message_groups and messages.
  4. --dry-run does everything inside a transaction and rolls back, reporting the counts it would insert.

Run with the production URL only after a manual snapshot, off-peak, smallest agency first, and with the
delete_old_emails beat task paused (recovered groups carry their original dates).
"""

from __future__ import annotations

import argparse
import collections
import csv
import datetime as dt
import gzip
import io
import json
import sys
import time
import uuid
from pathlib import Path

import psycopg2
import psycopg2.extras

csv.field_size_limit(1 << 30)
METHOD = 'email-mandrill'
# How far apart two sends to the same address in the same group can be and still be the same email.
# Sized from the rendered data: 6,930 near-duplicate pairs, p99 2s, 6,902 of them under 5s.
DEDUPE_WINDOW = 5.0


def log(msg: str) -> None:
    print(f'[{dt.datetime.now():%H:%M:%S}] {msg}', flush=True)


def read_rows(
    input_dir: Path,
    agency: str,
    start: dt.date | None,
    end: dt.date | None,
    as_code=None,
    start_ts: str | None = None,
    end_ts: str | None = None,
):
    """Rows for one agency, optionally bounded by day (--start/--end) and by time (--start-ts/--end-ts).

    The timestamp bounds exist for the Route A tail. A subaccount restored from a nightly snapshot
    was deleted some hours after that snapshot was taken, so Route B supplies only the sends in
    between. A whole-day filter would also pick up what it sent *after* the deletion, and those rows
    already exist in the database as real messages — the insert guard would not catch them, because
    Route B's send_ts comes from a log line and never matches the database's to the microsecond.
    Both bounds are inclusive and compare as ISO strings, which is what the rendered files hold.
    """
    files = sorted(input_dir.glob('????-??-??.csv.gz'))
    for f in files:
        day = dt.date.fromisoformat(f.stem[:10])
        if (start and day < start) or (end and day > end):
            continue
        with gzip.open(f, 'rt', newline='', encoding='utf-8') as fh:
            for r in csv.DictReader(fh):
                if start_ts and r['send_ts'] < start_ts:
                    continue
                if end_ts and r['send_ts'] > end_ts:
                    continue
                if r['company_code'] == agency:
                    if as_code:
                        new_code, new_branch = as_code
                        old_tag = f'{r["company_code"]}-{r["branch_id"]}'
                        new_tag = f'{new_code}-{new_branch}'
                        r['tags'] = r['tags'].replace(old_tag, new_tag)
                        r['company_code'], r['branch_id'] = new_code, new_branch
                    yield r


def majority_owner(input_dir: Path) -> dict[str, str]:
    """Group uuids used by more than one company code, mapped to the code that holds the most rows.

    54 uuids in the rendered set appear under two agencies. The loader reuses an existing group but
    refuses when its company is not the one the rows claim, so the minority rows block the whole
    agency: 8 of 25 loads refuse, 240,759 messages, over 33 rows. Resolving it as the loads run --
    first one to reach a uuid keeps it -- is order-dependent and would re-home 1,315 of one agency's
    rows into another's group, so ownership is decided here, once, from the entire input.

    The scan deliberately ignores --start/--end and --start-ts/--end-ts: the answer must not change
    between the main load and the Route A tail run. Ties break on the code to stay deterministic.
    """
    counts: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for f in sorted(input_dir.glob('????-??-??.csv.gz')):
        with gzip.open(f, 'rt', newline='', encoding='utf-8') as fh:
            for r in csv.DictReader(fh):
                if r['group_uuid']:
                    counts[r['group_uuid']][f'{r["company_code"]}:{r["branch_id"]}'] += 1
    return {u: min(c.items(), key=lambda kv: (-kv[1], kv[0]))[0] for u, c in counts.items() if len(c) > 1}


def merge_db_owners(rendered: dict[str, str], db: dict[str, str]) -> dict[str, str]:
    """Ownership from the rendered files, overlaid with what the database already holds.

    The database wins. Its groups are real rows -- Route A's restored history, or live sends -- so a
    rendered row carrying one of their uuids is the one that has borrowed it, whatever the rendered
    majority says. The snapshot restore brings in 57,705 real groups, one of which a reconstructed
    agency also carries.
    """
    return {**rendered, **db}


def group_uuid_for(r: dict, owners: dict[str, str] | None = None) -> str:
    """The group a row belongs in: its own uuid, unless that uuid belongs to another agency.

    A row whose uuid is owned by a different company code falls through to the synthesised path, the
    same one rows with no uuid take, giving that agency its own group instead of borrowing one.
    """
    code = f'{r["company_code"]}:{r["branch_id"]}'
    if r['group_uuid'] and (owners is None or owners.get(r['group_uuid'], code) == code):
        return r['group_uuid']
    minute = r['send_ts'][:16]
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f'recovered:{r["company_code"]}:{r["branch_id"]}:{minute}'))


def parse_ts(s: str) -> dt.datetime:
    """One rendered send_ts as an aware UTC datetime. A value without an offset is UTC, which is the
    same assumption the COPY step makes when it appends +00:00."""
    ts = dt.datetime.fromisoformat(s)
    return ts.replace(tzinfo=dt.timezone.utc) if ts.tzinfo is None else ts.astimezone(dt.timezone.utc)


def dedupe_rows(
    rows: list[dict], window_seconds: float = DEDUPE_WINDOW, owners: dict[str, str] | None = None
) -> list[dict]:
    """Collapse rows for the same (group, to_address) sent within window_seconds, keeping the first.

    The insert guard in load() skips staged rows that already exist in `messages`, but it cannot see
    rows inserted by its own statement. Duplicates are adjacent once rows are sorted by send_ts, and
    COPY preserves that order into the staging heap, so a duplicate pair almost always lands in the
    same --batch-size chunk and both rows would be written. Subject is deliberately excluded from the
    key so this pass and the SQL guard agree on what a duplicate is.

    The window is not cosmetic. The rendered files were merged from two sources whose clocks differ,
    so the same email appears twice a few milliseconds apart about as often as it appears twice
    identically — 6,930 such pairs against 38,383 exact ones, median 43ms, p99 2s. An exact key
    collapses only the second kind and writes the first kind twice.

    Each row is measured against the last row *kept*, not its predecessor, so a long run of sends a
    few seconds apart cannot chain into a single kept row and swallow genuinely distinct email.
    window_seconds=0 restores the old exact-match behaviour.
    """
    kept_at: dict[tuple[str, str], dt.datetime] = {}
    deduped = []
    for r in rows:
        key = (group_uuid_for(r, owners), r['to_address'])
        ts = parse_ts(r['send_ts'])
        previous = kept_at.get(key)
        if previous is not None and abs((ts - previous).total_seconds()) <= window_seconds:
            continue
        kept_at[key] = ts
        deduped.append(r)
    return deduped


def existing_group_conflicts(
    groups: dict[str, dict], group_company: dict[str, int], company_id: dict[str, int]
) -> list[tuple[str, int, int]]:
    """Group uuids already in the database whose company is not the one the rows belong to.

    An existing group is reused untouched, but messages.company_id comes from the CSV row, so a
    mismatch would file one agency's mail under another agency's group. Returns (uuid, db, expected).
    """
    conflicts = []
    for u, g in groups.items():
        db_company = group_company.get(u)
        if db_company is not None and db_company != company_id[g['code']]:
            conflicts.append((u, db_company, company_id[g['code']]))
    return conflicts


def positive_int(v: str) -> int:
    """--batch-size 0 makes `limit 0` drain nothing from staging, so the insert loop never ends."""
    n = int(v)
    if n < 1:
        raise argparse.ArgumentTypeError(f'must be 1 or more, got {n}')
    return n


def non_negative_float(v: str) -> float:
    """A negative window would make the BETWEEN range empty, silently disabling the guard."""
    f = float(v)
    if f < 0:
        raise argparse.ArgumentTypeError(f'must be 0 or more, got {f}')
    return f


def rollback_batch(cur, batch_id: str, dry_run: bool) -> None:
    """Remove a batch: its messages, and any group that exists only because of it.

    A group belongs to the batch if every message in it carries this batch id — which is true of the groups
    the load created, and false for a group that already held real sends (so those are always left alone).
    This is worked out before the messages are deleted, while the evidence still exists.
    """
    cur.execute(
        """select distinct m.group_id from messages m
           where m.extra->>'recovered_batch' = %(b)s
             and not exists (select 1 from messages o
                             where o.group_id = m.group_id
                               and (o.extra->>'recovered_batch') is distinct from %(b)s)""",
        {'b': batch_id},
    )
    group_ids = [r[0] for r in cur.fetchall()]
    cur.execute("select count(*) from messages where extra->>'recovered_batch' = %s", (batch_id,))
    n_msg = cur.fetchone()[0]
    log(f'batch {batch_id}: {n_msg} messages and {len(group_ids)} groups to delete')
    if dry_run:
        return
    cur.execute("delete from messages where extra->>'recovered_batch' = %s", (batch_id,))
    n_grp = 0
    if group_ids:
        cur.execute(
            """delete from message_groups where id = any(%s)
               and not exists (select 1 from messages m where m.group_id = message_groups.id)""",
            (group_ids,),
        )
        n_grp = cur.rowcount
    drop_legacy_group_table(cur, batch_id)
    log(f'batch {batch_id}: deleted {n_msg} messages and {n_grp} now-empty groups')


def drop_legacy_group_table(cur, batch_id: str) -> None:
    """Earlier versions recorded created groups in a recovered_batch_groups table.

    Nothing writes it any more. Clear this batch's rows and drop the table once it is empty, so a database
    that was loaded with an older copy of this script does not keep an unused table for ever.
    """
    cur.execute("select to_regclass('recovered_batch_groups')")
    if not cur.fetchone()[0]:
        return
    cur.execute('delete from recovered_batch_groups where batch_id = %s', (batch_id,))
    cur.execute('select count(*) from recovered_batch_groups')
    if cur.fetchone()[0] == 0:
        cur.execute('drop table recovered_batch_groups')
        log('dropped the now-empty legacy recovered_batch_groups table')


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--dsn', required=True)
    ap.add_argument('--input', type=Path, help='folder of rendered/<date>.csv.gz files')
    ap.add_argument('--agency', help='agency code exactly as it appears in the rendered files')
    ap.add_argument('--batch-id', help='label stamped on every inserted row (extra.recovered_batch)')
    ap.add_argument('--start', type=dt.date.fromisoformat)
    ap.add_argument('--end', type=dt.date.fromisoformat)
    ap.add_argument(
        '--start-ts',
        metavar='ISO_TS',
        help='only rows with send_ts >= this (inclusive), e.g. 2026-08-26T01:06:00. '
        'For the Route A tail, where a whole-day cut would re-insert sends that '
        'already exist in the database.',
    )
    ap.add_argument('--end-ts', metavar='ISO_TS', help='only rows with send_ts <= this (inclusive)')
    ap.add_argument('--batch-size', type=positive_int, default=5000)
    ap.add_argument(
        '--dedupe-window',
        type=non_negative_float,
        default=DEDUPE_WINDOW,
        metavar='SECONDS',
        help=f'treat two sends to the same address in the same group as the same email when they are '
        f'within this many seconds (default {DEDUPE_WINDOW}). Applies both to the input and to '
        'the check against what the database already holds. 0 means exact match only, which '
        're-inserts live email whose timestamp differs by microseconds.',
    )
    ap.add_argument(
        '--method',
        default=METHOD,
        help=f'message method to store (default {METHOD}). Use email-test to view a batch in a local '
        'dev TC2, whose test email backend queries Morpheus for email-test — never for production.',
    )
    ap.add_argument(
        '--as-code',
        metavar='CODE:BRANCH',
        help='load under this company code instead of the one in the files, e.g. testagency:3 '
        '(local testing only — it re-homes the emails onto another agency/branch)',
    )
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--rollback', metavar='BATCH_ID', help='delete everything stamped with this batch id and exit')
    args = ap.parse_args()

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = False
    cur = conn.cursor()
    try:
        if args.rollback:
            rollback_batch(cur, args.rollback, args.dry_run)
            conn.rollback() if args.dry_run else conn.commit()
            return 0
        if not (args.input and args.agency and args.batch_id):
            sys.exit('--input, --agency and --batch-id are required to load')
        return load(conn, cur, args)
    finally:
        conn.close()


def load(conn, cur, args) -> int:
    t0 = time.monotonic()
    as_code = None
    if args.as_code:
        c, _, b = args.as_code.partition(':')
        if not c or not b:
            sys.exit('--as-code must look like CODE:BRANCH, e.g. testagency:3')
        as_code = (c, b)
    rows = list(read_rows(args.input, args.agency, args.start, args.end, as_code, args.start_ts, args.end_ts))
    if not rows:
        log(f'no rows for {args.agency} in {args.input}')
        return 0
    rows.sort(key=lambda r: r['send_ts'])
    # Decided from the whole input, before anything is written, so the answer cannot depend on
    # which agencies have already been loaded.
    owners = majority_owner(args.input)
    cur.execute(
        """select g.uuid::text, c.code from message_groups g join companies c on c.id = g.company_id
           where g.uuid = any(%s::uuid[])""",
        (sorted({r['group_uuid'] for r in rows if r['group_uuid']}),),
    )
    owners = merge_db_owners(owners, dict(cur.fetchall()))
    borrowed = sum(
        1
        for r in rows
        if r['group_uuid'] and owners.get(r['group_uuid'], '') not in ('', f'{r["company_code"]}:{r["branch_id"]}')
    )
    if borrowed:
        log(f'groups: {borrowed} row(s) carry a uuid owned by another agency; giving them their own group')
    deduped = dedupe_rows(rows, args.dedupe_window, owners)
    if len(deduped) != len(rows):
        log(
            f'input: dropped {len(rows) - len(deduped)} duplicate rows (same group and address within {args.dedupe_window}s)'
        )
    rows = deduped
    log(f'{args.agency}: {len(rows)} rows from {rows[0]["send_ts"][:10]} to {rows[-1]["send_ts"][:10]}')

    # 1. companies
    codes = sorted({f'{r["company_code"]}:{r["branch_id"]}' for r in rows})
    company_id = {}
    for code in codes:
        cur.execute('insert into companies (code) values (%s) on conflict (code) do nothing', (code,))
        cur.execute('select id from companies where code = %s', (code,))
        company_id[code] = cur.fetchone()[0]
    log(f'companies: {company_id}')

    # 2. groups
    groups: dict[str, dict] = {}
    for r in rows:
        u = group_uuid_for(r, owners)
        g = groups.setdefault(
            u,
            dict(
                code=f'{r["company_code"]}:{r["branch_id"]}',
                ts=r['send_ts'],
                from_email=r['from_address'] or None,
                synth=not r['group_uuid'],
            ),
        )
        g['ts'] = min(g['ts'], r['send_ts'])
    cur.execute('select uuid::text, id, company_id from message_groups where uuid = any(%s::uuid[])', (list(groups),))
    existing = cur.fetchall()
    group_id = {u: gid for u, gid, _ in existing}
    existing_groups = len(group_id)
    conflicts = existing_group_conflicts(groups, {u: cid for u, _, cid in existing}, company_id)
    if conflicts:
        for u, db_company, expected in conflicts[:10]:
            log(f'  group {u} belongs to company {db_company}, but its rows are company {expected}')
        sys.exit(
            f'{len(conflicts)} existing group(s) belong to a different company than the rows claim — refusing to '
            'load. Check --agency and --as-code against the target database.'
        )
    new_groups = [(u, g) for u, g in groups.items() if u not in group_id]
    for u, g in new_groups:
        cur.execute(
            'insert into message_groups (uuid, company_id, message_method, created_ts, from_email) values (%s, %s, %s, %s, %s) returning id',
            (u, company_id[g['code']], args.method, g['ts'], g['from_email']),
        )
        group_id[u] = cur.fetchone()[0]
    log(
        f'groups: {len(groups)} in files, {existing_groups} already present, {len(new_groups)} created ({sum(g["synth"] for _, g in new_groups)} synthesised)'
    )

    # 3. messages via staging
    cur.execute(
        """create temp table staging_messages (
             group_uuid text, group_id int, company_id int, send_ts timestamptz, to_first_name text, to_last_name text,
             to_address text, tags text[], subject text, body text, extra jsonb) on commit drop"""
    )
    buf = io.StringIO()
    w = csv.writer(buf)
    for r in rows:
        u = group_uuid_for(r, owners)
        code = f'{r["company_code"]}:{r["branch_id"]}'
        tags = json.loads(r['tags'] or '[]')
        tags = [u] + [t for t in tags if t != u]
        extra = dict(
            recovered_batch=args.batch_id,
            recovered_from='papertrail+bigquery',
            trigger=r['trigger'] or None,
            user_id=int(r['user_id']) if r['user_id'] else None,
            role_type=r['role_type'] or None,
            sources=json.loads(r['sources'] or '{}'),
            context_keys=json.loads(r['context_keys'] or '[]'),
            group_synthesised=not r['group_uuid'],
            reply_to=r['reply_to'] or None,
        )
        w.writerow(
            [
                u,
                group_id[u],
                company_id[code],
                r['send_ts'] + ('' if '+' in r['send_ts'] or r['send_ts'].endswith('Z') else '+00:00'),
                r['first_name'] or '',
                r['last_name'] or '',
                r['to_address'] or '',
                '{' + ','.join('"' + t.replace('\\', '\\\\').replace('"', '\\"') + '"' for t in tags) + '}',
                r['subject'],
                r['body'],
                json.dumps(extra),
            ]
        )
    buf.seek(0)
    cur.copy_expert('copy staging_messages from stdin with (format csv)', buf)
    cur.execute(
        "update staging_messages set to_first_name = nullif(to_first_name, ''), to_last_name = nullif(to_last_name, ''), to_address = nullif(to_address, '')"
    )
    window = f'{args.dedupe_window} seconds'
    cur.execute(
        """select count(*) from staging_messages s where exists (
             select 1 from messages m where m.group_id = s.group_id and m.to_address is not distinct from s.to_address
               and m.send_ts between s.send_ts - %(w)s::interval and s.send_ts + %(w)s::interval)""",
        {'w': window},
    )
    dupes = cur.fetchone()[0]
    log(f'staged {len(rows)} rows; {dupes} already exist (same group and address within {window}) and will be skipped')

    inserted = 0
    while True:
        cur.execute(
            """with batch as (
                 delete from staging_messages s where ctid in (select ctid from staging_messages limit %(n)s) returning *
               )
               insert into messages (group_id, company_id, method, send_ts, update_ts, status, to_first_name, to_last_name, to_address, tags, subject, body, extra)
               select b.group_id, b.company_id, %(method)s, b.send_ts, b.send_ts, 'send', b.to_first_name, b.to_last_name, b.to_address, b.tags, b.subject, b.body, b.extra
               from batch b
               where not exists (select 1 from messages m where m.group_id = b.group_id and m.to_address is not distinct from b.to_address
                                   and m.send_ts between b.send_ts - %(w)s::interval and b.send_ts + %(w)s::interval)""",
            {'n': args.batch_size, 'method': args.method, 'w': window},
        )
        n = cur.rowcount
        cur.execute('select count(*) from staging_messages')
        left = cur.fetchone()[0]
        inserted += n
        log(f'  inserted {n} ({inserted} so far, {left} staged rows left)')
        if left == 0:
            break

    if args.dry_run:
        conn.rollback()
        log(
            f'DRY RUN: would insert {inserted} messages, {len(new_groups)} groups, {len([c for c in codes])} company codes for {args.agency}; rolled back'
        )
    else:
        conn.commit()
        log(
            f'COMMITTED batch {args.batch_id}: {inserted} messages, {len(new_groups)} groups for {args.agency} in {time.monotonic() - t0:.0f}s'
        )
        log(
            'note: the message_aggregation view refreshes hourly; refresh manually if the dashboard should update sooner'
        )
    return 0


if __name__ == '__main__':
    sys.exit(main())
