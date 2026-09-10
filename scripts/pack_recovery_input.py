#!/usr/bin/env python3
"""
Pack the recovery inputs into the two archives the remote loader downloads from S3.

    uv run --with pyarrow python scripts/pack_recovery_input.py \
        --recovery-dir "/Users/dan/Documents/Morph email recovery" --out /tmp/s3_upload

Run once, locally. The recovery data lives only on the laptop that built it and the dyno starts with
nothing but the app's slug, so the input has to be delivered somehow -- this is the packing half of
that, and scripts/recover_from_s3.py is the unpacking half.

Two archives out:

  route_a.tar.gz   the snapshot export, as gzipped JSON Lines, one file per table
  route_b.tar.gz   the rendered CSVs, exactly as the pipeline wrote them

**Route A is converted from parquet to JSON Lines here.** Reading parquet needs pyarrow, a ~45 MB
dependency this app does not have and would have to gain permanently to serve a one-off recovery.
JSON Lines needs nothing but the standard library, so the dyno stays clean.

Not CSV, though, and the distinction matters: CSV cannot express null. It writes an empty field for
both None and '', and the snapshot has columns that are genuinely one or the other -- external_id,
subject, to_address. Restoring a null as an empty string would change the data, and Route A's whole
premise is that the rows go back exactly as they were. JSON Lines keeps null, int and bool intact.

Route B needs no conversion. Its input is already one row per email in gzipped CSV, and it is read
by csv.DictReader either way, so those files are tarred untouched.

What is deliberately NOT included: route_a/message_groups, 347 MB of *unfiltered* snapshot groups.
restore_from_snapshot.py reads only `companies` out of that folder; the groups it loads come from
route_a_filtered. It has never been needed, and there is no reason to put another 347 MB of customer
data in S3.

The output is real customer email -- bodies, subjects and recipient addresses, including for people
who are not themselves TutorCruncher customers. Upload it to a private, encrypted prefix and delete
it once the load is verified.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import shutil
import tarfile
import tempfile
from pathlib import Path

# (path inside the archive, folder under the recovery dir holding the parquet parts). The layout
# mirrors what restore_from_snapshot.py expects on disk: the four data tables under a folder passed
# as --input, and `companies` in a route_a folder beside it.
ROUTE_A_TABLES = [
    ('route_a_filtered/message_groups', 'route_a_filtered/message_groups'),
    ('route_a_filtered/messages', 'route_a_filtered/messages'),
    ('route_a_filtered/events', 'route_a_filtered/events'),
    ('route_a_filtered/links', 'route_a_filtered/links'),
    ('route_a/companies', 'route_a/companies'),
]


def log(msg: str) -> None:
    print(f'[{dt.datetime.now():%H:%M:%S}] {msg}', flush=True)


def human(n: float) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if n < 1024 or unit == 'GB':
            return f'{n:.0f} {unit}' if unit == 'B' else f'{n:.1f} {unit}'
        n /= 1024
    return f'{n:.1f} GB'


def sha256(path: Path) -> str:
    """Checksum so the download can be proved to match what was uploaded."""
    h = hashlib.sha256()
    with path.open('rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def json_safe(v):
    """Anything parquet hands back that json cannot encode, as a string.

    In practice the export's timestamps already arrive as strings; this is a backstop so an
    unexpected type fails loudly at pack time rather than silently at load time.
    """
    if isinstance(v, (dt.datetime, dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, bytes):
        return v.decode('utf-8', 'replace')
    return str(v)


def parquet_to_jsonl(src: Path, dest: Path) -> int:
    """One table's parquet parts as a single gzipped JSON Lines file.

    Written a part at a time rather than concatenating the whole table first: `messages` is 117,784
    rows of email bodies and holding all of it as an Arrow table peaks at 1.4 GB.
    """
    import pyarrow.parquet as pq

    parts = sorted(src.rglob('*.parquet'))
    if not parts:
        raise SystemExit(f'no parquet files under {src}')
    dest.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    with gzip.open(dest, 'wt', encoding='utf-8') as fh:
        for part in parts:
            for row in pq.read_table(part).to_pylist():
                fh.write(json.dumps(row, default=json_safe) + '\n')
                rows += 1
    log(f'  {dest.name}: {len(parts):,} parts -> {rows:,} rows, {human(dest.stat().st_size)}')
    return rows


def build_route_a(recovery: Path, staging: Path, out: Path) -> dict[str, int]:
    counts = {}
    for inner, folder in ROUTE_A_TABLES:
        table = Path(inner).name
        counts[table] = parquet_to_jsonl(recovery / folder, staging / inner / f'{table}.jsonl.gz')
    with tarfile.open(out, 'w:gz') as tar:
        for inner, _ in ROUTE_A_TABLES:
            table = Path(inner).name
            tar.add(staging / inner / f'{table}.jsonl.gz', arcname=f'{inner}/{table}.jsonl.gz')
    log(f'  route_a.tar.gz: {human(out.stat().st_size)}')
    return counts


def build_route_b(recovery: Path, out: Path) -> int:
    """Tar the rendered CSVs. Each is already gzipped, so the tar itself is stored uncompressed --
    gzipping gzip costs a minute and saves nothing."""
    files = sorted((recovery / 'rendered').glob('????-??-??.csv.gz'))
    if not files:
        raise SystemExit(f'no rendered csv.gz files under {recovery / "rendered"}')
    with tarfile.open(out, 'w') as tar:
        for f in files:
            tar.add(f, arcname=f'rendered/{f.name}')
    log(f'  route_b.tar.gz: {len(files)} files, {human(out.stat().st_size)}')
    return len(files)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--recovery-dir', type=Path, required=True, help='the "Morph email recovery" folder')
    ap.add_argument('--out', type=Path, required=True, help='where to write the two archives')
    args = ap.parse_args()

    if not args.recovery_dir.exists():
        raise SystemExit(f'{args.recovery_dir} does not exist')
    args.out.mkdir(parents=True, exist_ok=True)

    staging = Path(tempfile.mkdtemp(prefix='pack_recovery_'))
    try:
        log('route A: parquet -> gzipped json lines')
        counts = build_route_a(args.recovery_dir, staging, args.out / 'route_a.tar.gz')
        log('route B: rendered csv, unchanged')
        build_route_b(args.recovery_dir, args.out / 'route_b.tar.gz')
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    log('')
    lines, total = [], 0
    for name in ('route_a.tar.gz', 'route_b.tar.gz'):
        path = args.out / name
        size = path.stat().st_size
        total += size
        digest = sha256(path)
        lines.append(f'{digest}  {size}  {name}')
        log(f'  {digest[:16]}…  {human(size):>9}  {name}')
    (args.out / 'MANIFEST.txt').write_text('\n'.join(lines) + '\n')

    log('')
    log('route A row counts: ' + ', '.join(f'{t} {n:,}' for t, n in counts.items()))
    log(f'2 archives, {human(total)} total, in {args.out}')


if __name__ == '__main__':
    main()
