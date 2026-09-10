#!/usr/bin/env python3
"""
Run the email recovery from S3, on a dyno, instead of from a laptop.

    python scripts/recover_from_s3.py --phase route-a
    python scripts/recover_from_s3.py --phase route-b
    python scripts/recover_from_s3.py --phase tails
    python scripts/recover_from_s3.py --phase all --dry-run

Route B alone is eleven minutes of writes against production. Run from a laptop, a dropped wifi
connection or a sleeping machine interrupts it -- nothing is corrupted, since every agency is its own
transaction, but you come back to a terminal that died on agency 19 and have to work out where you
were. This runs the identical loaders on a one-off dyno, which does not care about your laptop.

Config, all from the environment:

    DATABASE_URL                    already set on the app; the loaders are never given a DSN by hand
    AWS_ACCESS_KEY_ID               read-only key for the recovery prefix
    AWS_SECRET_ACCESS_KEY
    AWS_EMAIL_RECOVERY_BUCKET       default tutorcruncher-dev-private
    AWS_EMAIL_RECOVERY_PREFIX       default email-recovery

Which agencies to load, in what order, and which of them take only a bounded window, come from
agencies_to_load.json in the same S3 prefix. That list is data rather than code so no customer names
are committed to this repo -- but the rule it must obey is enforced here, in parse_agency_list: an
agency that takes a bounded window must never also appear in the full-history list.

What it does NOT do is reimplement the loading. It downloads the two archives, unpacks them into the
layout the loaders expect, and then invokes scripts/restore_from_snapshot.py and
scripts/load_recovered.py as separate processes -- exactly the commands rehearsed by hand, one
process per agency, so a failure kills one agency's transaction and nothing else. Their output is
this script's output.

Resuming is simply running it again. Every agency is its own transaction and the loaders skip rows
that are already present, so a re-run after a dyno restart costs seconds per completed agency. There
is no progress file, because a progress file can disagree with the database and the database cannot
disagree with itself.

The phases in order, which is also what --phase all does:

  route-a   restore the real rows for the agencies whose deletion happened after the snapshot
  route-b   every agency in the full-history list, smallest first
  tails     the bounded windows -- what the snapshot-restored agencies sent between the snapshot
            being taken and their account being deleted

Read §1a and §3a of ROUTE_B_RUNBOOK.md before running this against production.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

SCRIPTS = Path(__file__).parent
DEFAULT_BUCKET = 'tutorcruncher-dev-private'
DEFAULT_PREFIX = 'email-recovery'

ROUTE_A_ARCHIVE = 'route_a.tar.gz'
ROUTE_B_ARCHIVE = 'route_b.tar.gz'

# Which agencies to load, in what order, and which of them get only a bounded window. Downloaded
# alongside the data rather than written here, so no customer names are committed to this repo.
AGENCY_LIST = 'agencies_to_load.json'

PHASE_ARCHIVES = {
    'route-a': [ROUTE_A_ARCHIVE],
    'route-b': [ROUTE_B_ARCHIVE],
    'tails': [ROUTE_B_ARCHIVE],
    'all': [ROUTE_A_ARCHIVE, ROUTE_B_ARCHIVE],
}

PARTIAL_FIELDS = ('agency', 'day', 'start_ts', 'end_ts')


def log(msg: str) -> None:
    print(f'[{dt.datetime.now():%H:%M:%S}] {msg}', flush=True)


def database_url() -> str:
    """The app's own database, normalised.

    Deliberately not a --dsn argument: the one irreversible mistake available here is pointing a
    load at the wrong database, and reading the app's own environment removes the opportunity.
    """
    url = os.environ.get('DATABASE_URL')
    if not url:
        sys.exit('DATABASE_URL is not set. This is meant to run on a dyno, where the app provides it.')
    return url.replace('postgres://', 'postgresql://', 1) if url.startswith('postgres://') else url


def archives_for(phase: str) -> list[str]:
    if phase not in PHASE_ARCHIVES:
        sys.exit(f'unknown phase {phase!r}; expected one of {", ".join(PHASE_ARCHIVES)}')
    return PHASE_ARCHIVES[phase]


def parse_agency_list(doc: dict) -> tuple[list[str], list[dict]]:
    """The downloaded list, checked hard enough that a bad one stops the run rather than mis-loading.

    The names are data, but the rule they have to obey is not: **an agency that gets only a bounded
    window must never also appear in the full-history list**. Those agencies had their real email
    restored from the database snapshot, and loading their whole rendered history would file
    reconstructed copies alongside the originals. The check below is what keeps that guarantee in
    the code where it can be reviewed, without the code needing to know anyone's name.
    """
    agencies = (doc.get('agencies') or {}).get('codes')
    partial = (doc.get('partial_agencies') or {}).get('entries', [])
    if not agencies or not isinstance(agencies, list):
        sys.exit(f'{AGENCY_LIST}: no agencies.codes list')
    if len(set(agencies)) != len(agencies):
        sys.exit(f'{AGENCY_LIST}: agencies.codes contains duplicates')
    for entry in partial:
        missing = [f for f in PARTIAL_FIELDS if not entry.get(f)]
        if missing:
            sys.exit(f'{AGENCY_LIST}: a partial_agencies entry is missing {", ".join(missing)}')
    overlap = sorted(set(agencies) & {e['agency'] for e in partial})
    if overlap:
        sys.exit(
            f'{AGENCY_LIST}: {len(overlap)} agency(s) appear in both agencies.codes and '
            'partial_agencies. An agency that gets only a bounded window must never have its whole '
            'history loaded too -- that would duplicate email already restored from the snapshot. '
            'Refusing to run; nothing has been written.'
        )
    return agencies, partial


def agencies_to_run(agencies: list[str], only: str | None) -> list[str]:
    if only is None:
        return agencies
    if only not in agencies:
        sys.exit(f'{only!r} is not in the agency list. If it takes a bounded window, use --phase tails.')
    return [only]


def batch_id(agency: str, date: str) -> str:
    return f'recover-{agency}-{date}'


def download(bucket: str, prefix: str, name: str, dest: Path) -> None:
    import boto3

    key = f'{prefix.rstrip("/")}/{name}'
    log(f'  downloading s3://{bucket}/{key}')
    boto3.client('s3').download_file(bucket, key, str(dest))
    log(f'  {name}: {dest.stat().st_size / 1024**2:.1f} MB')


def extract(archive: Path, into: Path) -> None:
    with tarfile.open(archive) as tar:
        # filter='data' refuses absolute paths and parent-directory escapes in member names.
        tar.extractall(into, filter='data')
    archive.unlink()  # the dyno's disk is small and the tarball is dead weight once unpacked


def get(name: str, bucket: str, prefix: str, workdir: Path, archive_dir: Path | None) -> Path:
    """One object into the working directory, from S3 or from a local copy."""
    target = workdir / name
    if archive_dir:
        local = archive_dir / name
        if not local.exists():
            sys.exit(f'{local} not found')
        log(f'  using {local} ({local.stat().st_size / 1024**2:.1f} MB)')
        shutil.copy(local, target)
    else:
        download(bucket, prefix, name, target)
    return target


def fetch(phase: str, bucket: str, prefix: str, workdir: Path, archive_dir: Path | None = None) -> None:
    """Get the archives this phase needs and unpack them.

    --archive-dir skips the download and uses copies already on disk. It is how this is rehearsed
    against a local database without S3 credentials, and it saves re-fetching 108 MB when a phase is
    retried on a dyno that still has the previous download.
    """
    for name in archives_for(phase):
        extract(get(name, bucket, prefix, workdir, archive_dir), workdir)


def fetch_agency_list(
    bucket: str, prefix: str, workdir: Path, archive_dir: Path | None
) -> tuple[list[str], list[dict]]:
    path = get(AGENCY_LIST, bucket, prefix, workdir, archive_dir)
    return parse_agency_list(json.loads(path.read_text()))


def restore_command(dsn: str, workdir: Path, dry_run: bool) -> list[str]:
    cmd = [
        sys.executable,
        str(SCRIPTS / 'restore_from_snapshot.py'),
        '--dsn',
        dsn,
        '--input',
        str(workdir / 'route_a_filtered'),
    ]
    return cmd + ['--dry-run'] if dry_run else cmd


def load_command(
    dsn: str, workdir: Path, agency: str, batch: str, dry_run: bool, tail: dict | None = None
) -> list[str]:
    cmd = [
        sys.executable,
        str(SCRIPTS / 'load_recovered.py'),
        '--dsn',
        dsn,
        '--input',
        str(workdir / 'rendered'),
        '--agency',
        agency,
        '--batch-id',
        batch,
    ]
    if tail:
        cmd += [
            '--start', tail['day'],
            '--end', tail['day'],
            '--start-ts', tail['start_ts'],
            '--end-ts', tail['end_ts'],
        ]  # fmt: skip
    return cmd + ['--dry-run'] if dry_run else cmd


def run(cmd: list[str], what: str) -> None:
    """Run one loader. Its output is ours; a non-zero exit stops the campaign.

    Stopping matters: the loaders refuse rather than guess when something is wrong with the target,
    and ploughing on to the next agency would bury that refusal in the log.
    """
    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(f'\n{what} failed (exit {result.returncode}). Nothing was written for it. Fix, then re-run.')


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--phase', required=True, choices=sorted(PHASE_ARCHIVES))
    ap.add_argument('--only', metavar='AGENCY', help='one Route B agency, for retrying a single load')
    ap.add_argument(
        '--batch-date',
        default=dt.date.today().strftime('%Y%m%d'),
        help='stamped into every batch id (default today). Use the same value on a re-run '
        'so a retry lands in the batch it belongs to rather than a new one.',
    )
    ap.add_argument('--dry-run', action='store_true', help='passed to the loaders, which roll back')
    ap.add_argument('--bucket', default=os.environ.get('AWS_EMAIL_RECOVERY_BUCKET', DEFAULT_BUCKET))
    ap.add_argument('--prefix', default=os.environ.get('AWS_EMAIL_RECOVERY_PREFIX', DEFAULT_PREFIX))
    ap.add_argument(
        '--archive-dir', type=Path, metavar='DIR', help='use archives already on disk instead of downloading them'
    )
    args = ap.parse_args()

    if args.only and args.phase not in ('route-b', 'all'):
        sys.exit('--only picks one agency from the full-history list, so it needs --phase route-b')

    dsn = database_url()
    log(f'phase {args.phase}{" (dry run)" if args.dry_run else ""}, database {dsn.rsplit("@", 1)[-1]}')

    workdir = Path(tempfile.mkdtemp(prefix='recovery_'))
    try:
        all_agencies, partial = fetch_agency_list(args.bucket, args.prefix, workdir, args.archive_dir)
        log(f'  {AGENCY_LIST}: {len(all_agencies)} agencies, {len(partial)} loaded as a bounded window')
        fetch(args.phase, args.bucket, args.prefix, workdir, args.archive_dir)

        if args.phase in ('route-a', 'all'):
            log('route A: restoring the snapshot export')
            run(restore_command(dsn, workdir, args.dry_run), 'route A')

        if args.phase in ('route-b', 'all'):
            agencies = agencies_to_run(all_agencies, args.only)
            for i, agency in enumerate(agencies, 1):
                log(f'route B {i}/{len(agencies)}: {agency}')
                run(load_command(dsn, workdir, agency, batch_id(agency, args.batch_date), args.dry_run), agency)

        if args.phase in ('tails', 'all'):
            for entry in partial:
                expected = f' — expecting {entry["expected"]} messages' if entry.get('expected') else ''
                log(f'bounded window: {entry["agency"]}{expected}')
                batch = f'{entry["agency"]}-window-{args.batch_date}'
                run(
                    load_command(dsn, workdir, entry['agency'], batch, args.dry_run, tail=entry),
                    f'{entry["agency"]} bounded window',
                )
    finally:
        for path in sorted(workdir.rglob('*'), reverse=True):
            path.unlink() if path.is_file() else path.rmdir()
        workdir.rmdir()

    log('done' if not args.dry_run else 'dry run complete — nothing was written')


if __name__ == '__main__':
    main()
