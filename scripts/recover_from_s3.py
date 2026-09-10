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

  route-a   restore 549,261 real rows for the two agencies deleted after the 26 Aug snapshot
  route-b   25 agencies, 347,003 messages, smallest first
  tails     the sends those two Route A agencies made between the snapshot and their deletion

Read §1a and §3a of ROUTE_B_RUNBOOK.md before running this against production.
"""

from __future__ import annotations

import argparse
import datetime as dt
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

# Smallest first: the one-row agency is a live smoke test before the 87,909-row one. This is the
# order in ROUTE_B_RUNBOOK.md §3, and capital-tuition-group and prime-uk-education-ltd are absent
# from it deliberately -- they are Route A, and only their tail comes from here.
AGENCIES = [
    'tuition-with-chloe',
    'empowering-assessment-and-tuition',
    'teach-and-coach-tutors',
    'empowered-learning-llc',
    'tuition-extra-group',
    'tuition360',
    'tuition-central',
    'teach-me-islam-online-1',
    'london-tuition',
    'leetutors-llc',
    'lighthouse-global-education',
    'teaching-through-education',
    'lighthouse-tuition-south-west',
    'sarah-isaacs-english-tutoring-services',
    'tuition-point',
    'teach2teach',
    'lighthouse-learning',
    'london-home-tutors',
    'a-tutoring-services',
    'alist-1',
    'teachers-who-tutor',
    'simply-learns',
    'empower-tutoring-academy',
    'cardiff-vale-tutors-1',
    'simply-learning-tuition',
]

# The Route A tail. Both agencies were deleted hours after the 26 Aug 01:06 snapshot was taken, so
# the snapshot cannot hold what they sent in between -- 132 rows for Capital, 7 for Prime. The bounds
# are not optional: --start/--end are whole days, so without --start-ts/--end-ts the load would also
# insert what each sent *after* it was deleted, which is still live in production and which the
# insert guard cannot recognise, because a rendered send_ts comes from a log line rather than the app.
TAILS = [
    {
        'agency': 'capital-tuition-group',
        'day': '2026-08-26',
        'start_ts': '2026-08-26T01:06:00',
        'end_ts': '2026-08-26T19:48:37',
        'expected': 132,
    },
    {
        'agency': 'prime-uk-education-ltd',
        'day': '2026-08-26',
        'start_ts': '2026-08-26T01:06:00',
        'end_ts': '2026-08-26T02:08:31',
        'expected': 7,
    },
]

PHASE_ARCHIVES = {
    'route-a': [ROUTE_A_ARCHIVE],
    'route-b': [ROUTE_B_ARCHIVE],
    'tails': [ROUTE_B_ARCHIVE],
    'all': [ROUTE_A_ARCHIVE, ROUTE_B_ARCHIVE],
}


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


def agencies_to_run(only: str | None) -> list[str]:
    if only is None:
        return AGENCIES
    if only not in AGENCIES:
        sys.exit(
            f'{only!r} is not one of the 25 Route B agencies. '
            'capital-tuition-group and prime-uk-education-ltd are Route A -- use --phase tails.'
        )
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


def fetch(phase: str, bucket: str, prefix: str, workdir: Path, archive_dir: Path | None = None) -> None:
    """Get the archives this phase needs and unpack them.

    --archive-dir skips the download and uses archives already on disk. It is how this is rehearsed
    against a local database without S3 credentials, and it saves re-fetching 108 MB when a phase is
    retried on a dyno that still has the previous download.
    """
    for name in archives_for(phase):
        target = workdir / name
        if archive_dir:
            local = archive_dir / name
            if not local.exists():
                sys.exit(f'{local} not found')
            log(f'  using {local} ({local.stat().st_size / 1024**2:.1f} MB)')
            shutil.copy(local, target)
        else:
            download(bucket, prefix, name, target)
        extract(target, workdir)


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
        sys.exit('--only applies to the 25 Route B agencies, so it needs --phase route-b')

    dsn = database_url()
    log(f'phase {args.phase}{" (dry run)" if args.dry_run else ""}, database {dsn.rsplit("@", 1)[-1]}')

    workdir = Path(tempfile.mkdtemp(prefix='recovery_'))
    try:
        fetch(args.phase, args.bucket, args.prefix, workdir, args.archive_dir)

        if args.phase in ('route-a', 'all'):
            log('route A: restoring the snapshot export')
            run(restore_command(dsn, workdir, args.dry_run), 'route A')

        if args.phase in ('route-b', 'all'):
            agencies = agencies_to_run(args.only)
            for i, agency in enumerate(agencies, 1):
                log(f'route B {i}/{len(agencies)}: {agency}')
                run(load_command(dsn, workdir, agency, batch_id(agency, args.batch_date), args.dry_run), agency)

        if args.phase in ('tails', 'all'):
            for tail in TAILS:
                log(f'tail: {tail["agency"]} — expecting {tail["expected"]} messages')
                batch = f'{tail["agency"].split("-")[0]}-tail-{args.batch_date}'
                run(
                    load_command(dsn, workdir, tail['agency'], batch, args.dry_run, tail=tail),
                    f'{tail["agency"]} tail',
                )
    finally:
        for path in sorted(workdir.rglob('*'), reverse=True):
            path.unlink() if path.is_file() else path.rmdir()
        workdir.rmdir()

    log('done' if not args.dry_run else 'dry run complete — nothing was written')


if __name__ == '__main__':
    main()
