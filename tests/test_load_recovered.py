"""Unit tests for the recovery loader's pure helpers (scripts/load_recovered.py).

The script is run by hand against a --dsn and is never imported by the app, so it is loaded here by
path. Only the logic that decides *what* gets written is covered: a mistake in these three helpers
writes duplicate or mis-homed email history into production, which is exactly what the loader exists
to avoid.
"""

import argparse
import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parent.parent / 'scripts' / 'load_recovered.py'
_spec = importlib.util.spec_from_file_location('load_recovered', _SCRIPT)
load_recovered = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(load_recovered)


def row(send_ts, to_address='a@example.com', group_uuid='', code='agency', branch='1', subject='Reminder'):
    return {
        'send_ts': send_ts,
        'to_address': to_address,
        'group_uuid': group_uuid,
        'company_code': code,
        'branch_id': branch,
        'subject': subject,
    }


class TestDedupeRows:
    """The SQL insert guard cannot see rows inserted by its own statement, so identical rows inside
    one --batch-size chunk would both land. dedupe_rows is what stops that."""

    def test_collapses_rows_identical_on_group_address_and_send_ts(self):
        u = '11111111-1111-1111-1111-111111111111'
        rows = [row('2025-09-02T13:43:19+00:00', group_uuid=u)] * 5
        assert len(load_recovered.dedupe_rows(rows)) == 1

    def test_collapses_even_when_the_subject_differs(self):
        # The key deliberately excludes subject, matching the SQL guard. Keeping both copies here
        # would mean the guard and the python pass disagree about what a duplicate is.
        u = '11111111-1111-1111-1111-111111111111'
        rows = [
            row('2025-09-02T13:43:19+00:00', group_uuid=u, subject='One'),
            row('2025-09-02T13:43:19+00:00', group_uuid=u, subject='Two'),
        ]
        assert len(load_recovered.dedupe_rows(rows)) == 1

    def test_keeps_rows_differing_on_any_part_of_the_key(self):
        u = '11111111-1111-1111-1111-111111111111'
        v = '22222222-2222-2222-2222-222222222222'
        rows = [
            row('2025-09-02T13:43:19+00:00', group_uuid=u),
            # Comfortably outside the dedupe window: a second's difference now reads as the same
            # email logged by two sources, which is what p90 of the near-duplicate pairs looks like.
            row('2025-09-02T13:43:49+00:00', group_uuid=u),
            row('2025-09-02T13:43:19+00:00', to_address='b@example.com', group_uuid=u),
            row('2025-09-02T13:43:19+00:00', group_uuid=v),  # different group
        ]
        assert len(load_recovered.dedupe_rows(rows)) == 4

    def test_dedupes_rows_whose_group_is_synthesised(self):
        # No group_uuid: the group is derived from code + branch + minute, so two sends in the same
        # minute share a group and must still be compared on their exact send_ts.
        rows = [
            row('2025-09-02T13:43:19+00:00'),
            row('2025-09-02T13:43:19+00:00'),
            row('2025-09-02T13:43:45+00:00'),  # same synthesised group, different second
        ]
        assert len(load_recovered.dedupe_rows(rows)) == 2

    def test_does_not_merge_synthesised_groups_across_branches(self):
        rows = [
            row('2025-09-02T13:43:19+00:00', branch='1'),
            row('2025-09-02T13:43:19+00:00', branch='2'),
        ]
        assert len(load_recovered.dedupe_rows(rows)) == 2

    def test_keeps_the_first_occurrence_and_preserves_order(self):
        u = '11111111-1111-1111-1111-111111111111'
        rows = [
            row('2025-09-02T13:00:00+00:00', group_uuid=u, subject='first'),
            row('2025-09-02T13:00:00+00:00', group_uuid=u, subject='second'),
            row('2025-09-02T14:00:00+00:00', group_uuid=u, subject='third'),
        ]
        assert [r['subject'] for r in load_recovered.dedupe_rows(rows)] == ['first', 'third']

    def test_returns_input_unchanged_when_there_are_no_duplicates(self):
        rows = [row('2025-09-02T13:00:00+00:00'), row('2025-09-02T14:00:00+00:00')]
        assert load_recovered.dedupe_rows(rows) == rows


class TestExistingGroupConflicts:
    """A group_uuid already in the database is reused as-is, but messages.company_id comes from the
    CSV row. If those disagree the load writes one agency's mail under another agency's group."""

    def test_no_conflict_when_the_existing_group_matches(self):
        groups = {'u1': {'code': 'agency:1'}}
        assert load_recovered.existing_group_conflicts(groups, {'u1': 7}, {'agency:1': 7}) == []

    def test_reports_a_group_owned_by_a_different_company(self):
        groups = {'u1': {'code': 'agency:1'}}
        conflicts = load_recovered.existing_group_conflicts(groups, {'u1': 9}, {'agency:1': 7})
        assert conflicts == [('u1', 9, 7)]

    def test_ignores_groups_that_do_not_exist_yet(self):
        groups = {'u1': {'code': 'agency:1'}}
        assert load_recovered.existing_group_conflicts(groups, {}, {'agency:1': 7}) == []


class TestPositiveInt:
    """--batch-size 0 makes `limit 0` delete nothing from staging, so the insert loop never ends."""

    @pytest.mark.parametrize('value', ['0', '-1'])
    def test_rejects_values_below_one(self, value):
        with pytest.raises(argparse.ArgumentTypeError):
            load_recovered.positive_int(value)

    def test_accepts_a_positive_value(self):
        assert load_recovered.positive_int('5000') == 5000


class TestReadRows:
    """--start/--end are whole days. Route A needs a sub-day cut: a subaccount restored from a
    nightly snapshot was deleted some hours later the same day, so only the sends between the
    snapshot and the deletion come from Route B. Loading the whole day would also insert the sends
    made *after* the deletion, which already exist in the database as real messages."""

    @staticmethod
    def _write(tmp_path, rows):
        import csv
        import gzip

        path = tmp_path / '2026-08-26.csv.gz'
        with gzip.open(path, 'wt', newline='', encoding='utf-8') as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
        return tmp_path

    def test_start_ts_and_end_ts_cut_within_a_day(self, tmp_path):
        rows = [
            row('2026-08-26T00:30:00.000000', code='agency-a'),  # before the snapshot
            row('2026-08-26T01:32:43.507000', code='agency-a'),  # in the window
            row('2026-08-26T19:00:00.000000', code='agency-a'),  # in the window
            row('2026-08-26T22:39:09.472000', code='agency-a'),  # after the deletion
        ]
        d = self._write(tmp_path, rows)

        got = list(
            load_recovered.read_rows(
                d,
                'agency-a',
                None,
                None,
                start_ts='2026-08-26T01:06:00',
                end_ts='2026-08-26T19:48:37',
            )
        )
        assert [r['send_ts'] for r in got] == ['2026-08-26T01:32:43.507000', '2026-08-26T19:00:00.000000']

    def test_no_ts_bounds_returns_the_whole_day(self, tmp_path):
        rows = [
            row('2026-08-26T00:30:00.000000', code='agency-a'),
            row('2026-08-26T22:39:09.472000', code='agency-a'),
        ]
        d = self._write(tmp_path, rows)
        assert len(list(load_recovered.read_rows(d, 'agency-a', None, None))) == 2


class TestDedupeWindow:
    """The rendered files were merged from two sources (papertrail + bigquery) whose clocks differ,
    so the same email can appear twice a few milliseconds apart. dedupe_rows' exact key collapses
    38,383 identical pairs but misses 6,930 near-identical ones, which would then be written twice.
    The window is what closes that, and it must match the SQL guard's -- see TestInsertGuardWindow."""

    U = '11111111-1111-1111-1111-111111111111'

    def test_collapses_rows_a_few_milliseconds_apart(self):
        rows = [
            row('2026-08-26T01:32:43.507000', group_uuid=self.U),
            row('2026-08-26T01:32:43.550000', group_uuid=self.U),
        ]
        assert len(load_recovered.dedupe_rows(rows, window_seconds=5)) == 1

    def test_keeps_rows_further_apart_than_the_window(self):
        rows = [
            row('2026-08-26T01:32:43.507000', group_uuid=self.U),
            row('2026-08-26T01:32:52.000000', group_uuid=self.U),
        ]
        assert len(load_recovered.dedupe_rows(rows, window_seconds=5)) == 2

    def test_window_zero_keeps_the_old_exact_behaviour(self):
        rows = [
            row('2026-08-26T01:32:43.507000', group_uuid=self.U),
            row('2026-08-26T01:32:43.550000', group_uuid=self.U),
        ]
        assert len(load_recovered.dedupe_rows(rows, window_seconds=0)) == 2

    def test_window_does_not_merge_different_addresses_or_groups(self):
        rows = [
            row('2026-08-26T01:32:43.507000', group_uuid=self.U, to_address='a@example.com'),
            row('2026-08-26T01:32:43.510000', group_uuid=self.U, to_address='b@example.com'),
            row('2026-08-26T01:32:43.512000', group_uuid='22222222-2222-2222-2222-222222222222'),
        ]
        assert len(load_recovered.dedupe_rows(rows, window_seconds=5)) == 3

    def test_the_window_is_measured_from_the_row_that_was_kept(self):
        # Anchoring on the last *kept* row rather than the previous row stops a long run of sends a
        # few seconds apart from chaining into one, which would swallow genuinely distinct email.
        rows = [
            row('2026-08-26T01:00:00', group_uuid=self.U),
            row('2026-08-26T01:00:04', group_uuid=self.U),
            row('2026-08-26T01:00:08', group_uuid=self.U),
        ]
        kept = load_recovered.dedupe_rows(rows, window_seconds=5)
        assert [r['send_ts'] for r in kept] == ['2026-08-26T01:00:00', '2026-08-26T01:00:08']

    def test_offset_aware_and_naive_timestamps_compare_as_utc(self):
        # read_rows compares send_ts as strings, so both shapes reach here; a naive value is UTC.
        rows = [
            row('2026-08-26T01:32:43.507000', group_uuid=self.U),
            row('2026-08-26T01:32:43.550000+00:00', group_uuid=self.U),
        ]
        assert len(load_recovered.dedupe_rows(rows, window_seconds=5)) == 1


class TestInsertGuardWindow:
    """The SQL guard is what stops the loader re-inserting email the database already holds.

    It matched on an exact send_ts, but Route B's send_ts comes from a log line while the database's
    was set by the app, so they never agree to the microsecond. Every agency kept sending after its
    own subaccount was deleted, and the rendered files carry those post-deletion sends too -- so an
    exact match re-inserts, as reconstructions, thousands of emails that are still live in
    production. This runs the loader against a real database to prove the window closes that.

    No real agency codes or addresses appear here; the fixtures are invented.
    """

    GROUP = '33333333-3333-3333-3333-333333333333'
    ADDRESS = 'someone@example.com'

    @staticmethod
    def _rendered(tmp_path, send_ts, group_uuid, to_address):
        import csv
        import gzip

        path = tmp_path / '2026-08-26.csv.gz'
        fields = [
            'send_ts',
            'company_code',
            'branch_id',
            'group_uuid',
            'to_address',
            'first_name',
            'last_name',
            'user_id',
            'role_type',
            'trigger',
            'tags',
            'subject',
            'body',
            'from_address',
            'reply_to',
            'sources',
            'context_keys',
        ]
        with gzip.open(path, 'wt', newline='', encoding='utf-8') as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerow(
                {
                    'send_ts': send_ts,
                    'company_code': 'agency-a',
                    'branch_id': '1',
                    'group_uuid': group_uuid,
                    'to_address': to_address,
                    'first_name': 'Sam',
                    'last_name': 'Jones',
                    'user_id': '',
                    'role_type': '',
                    'trigger': 'reminder',
                    'tags': '[]',
                    'subject': 'Reminder',
                    'body': '<p>body</p>',
                    'from_address': 'from@example.com',
                    'reply_to': '',
                    'sources': '{}',
                    'context_keys': '[]',
                }
            )
        return tmp_path

    @staticmethod
    def _seed_real_send(cur, group_uuid, address, send_ts):
        """A send the agency made after its deletion: still live, so the loader must not re-add it."""
        cur.execute("insert into companies (code) values ('agency-a:1') returning id")
        company_id = cur.fetchone()[0]
        cur.execute(
            """insert into message_groups (uuid, company_id, message_method, created_ts)
               values (%s, %s, 'email-mandrill', %s) returning id""",
            (group_uuid, company_id, send_ts),
        )
        group_id = cur.fetchone()[0]
        cur.execute(
            """insert into messages (group_id, company_id, method, send_ts, update_ts, status, to_address, subject, body)
               values (%s, %s, 'email-mandrill', %s, %s, 'send', %s, 'Reminder', '<p>body</p>')""",
            (group_id, company_id, send_ts, send_ts, address),
        )

    def _args(self, tmp_path, window):
        return argparse.Namespace(
            input=tmp_path,
            agency='agency-a',
            batch_id='test-batch',
            start=None,
            end=None,
            start_ts=None,
            end_ts=None,
            as_code=None,
            batch_size=5000,
            method='email-mandrill',
            dry_run=False,
            dedupe_window=window,
        )

    def _run(self, tmp_path, window):
        import psycopg2

        from app.core.config import settings

        # The loader takes a raw psycopg2 connection and a --dsn, exactly as it is run by hand.
        conn = psycopg2.connect(settings.database_url)
        conn.autocommit = False
        cur = conn.cursor()
        try:
            self._seed_real_send(cur, self.GROUP, self.ADDRESS, '2026-08-26T22:39:09.472000+00:00')
            conn.commit()
            # 90ms earlier than the database's row: the same email, logged by a different clock.
            d = self._rendered(tmp_path, '2026-08-26T22:39:09.382000', self.GROUP, self.ADDRESS)
            load_recovered.load(conn, cur, self._args(d, window))
            cur.execute('select count(*) from messages')
            return cur.fetchone()[0]
        finally:
            conn.close()

    def test_a_near_match_is_recognised_as_already_present(self, db):
        assert self._run(self.tmp, 5) == 1

    def test_without_a_window_the_live_email_is_duplicated(self, db):
        # Documents the behaviour being fixed: an exact match never fires, so the row goes in twice.
        assert self._run(self.tmp, 0) == 2

    @pytest.fixture(autouse=True)
    def _tmp(self, tmp_path):
        self.tmp = tmp_path


class TestMajorityOwner:
    """54 group uuids appear under two different agencies, covering 2,202 rows of which 117 are the
    odd ones out. The loader refuses to reuse a group whose company is not the one its rows claim
    (existing_group_conflicts), so those 117 rows block 8 of the 25 agency loads outright -- 240,759
    messages, 68% of the recovery, stopped by 33 rows.

    Whoever loads first must not simply win the uuid: with the runbook's order that would re-home
    1,315 of one agency's rows into another's group. The owner is decided once, from the whole input,
    before any load -- the majority holder keeps the uuid and the minority rows are synthesised into
    their own group, which is what they should have had.
    """

    @staticmethod
    def _write(tmp_path, name, rows):
        import csv
        import gzip

        fields = ['send_ts', 'company_code', 'branch_id', 'group_uuid', 'to_address']
        with gzip.open(tmp_path / name, 'wt', newline='', encoding='utf-8') as fh:
            w = csv.DictWriter(fh, fieldnames=fields, extrasaction='ignore')
            w.writeheader()
            w.writerows(rows)
        return tmp_path

    U = '44444444-4444-4444-4444-444444444444'

    def test_only_shared_uuids_are_reported(self, tmp_path):
        d = self._write(
            tmp_path,
            '2026-08-26.csv.gz',
            [
                row('2026-08-26T01:00:00', group_uuid=self.U, code='agency-a'),
                row('2026-08-26T01:00:01', group_uuid='55555555-5555-5555-5555-555555555555', code='agency-b'),
            ],
        )
        assert load_recovered.majority_owner(d) == {}

    def test_the_code_with_the_most_rows_keeps_the_uuid(self, tmp_path):
        d = self._write(
            tmp_path,
            '2026-08-26.csv.gz',
            [
                row('2026-08-26T01:00:00', group_uuid=self.U, code='agency-a'),
                row('2026-08-26T01:00:01', group_uuid=self.U, code='agency-a'),
                row('2026-08-26T01:00:02', group_uuid=self.U, code='agency-b'),
            ],
        )
        assert load_recovered.majority_owner(d) == {self.U: 'agency-a:1'}

    def test_a_tie_is_broken_deterministically(self, tmp_path):
        d = self._write(
            tmp_path,
            '2026-08-26.csv.gz',
            [
                row('2026-08-26T01:00:00', group_uuid=self.U, code='agency-z'),
                row('2026-08-26T01:00:01', group_uuid=self.U, code='agency-a'),
            ],
        )
        assert load_recovered.majority_owner(d) == {self.U: 'agency-a:1'}

    def test_the_scan_covers_every_day_not_just_the_loaded_range(self, tmp_path):
        # --start-ts/--end-ts must not change who owns a uuid, or the Route A tail run would
        # disagree with the main load about which rows are the minority.
        self._write(tmp_path, '2026-08-26.csv.gz', [row('2026-08-26T01:00:00', group_uuid=self.U, code='agency-b')])
        d = self._write(
            tmp_path,
            '2026-08-27.csv.gz',
            [
                row('2026-08-27T01:00:00', group_uuid=self.U, code='agency-a'),
                row('2026-08-27T01:00:01', group_uuid=self.U, code='agency-a'),
            ],
        )
        assert load_recovered.majority_owner(d) == {self.U: 'agency-a:1'}


class TestGroupUuidForWithOwners:
    U = '44444444-4444-4444-4444-444444444444'

    def test_the_majority_keeps_the_real_uuid(self):
        r = row('2026-08-26T01:00:00', group_uuid=self.U, code='agency-a')
        assert load_recovered.group_uuid_for(r, {self.U: 'agency-a:1'}) == self.U

    def test_the_minority_is_synthesised_into_its_own_group(self):
        r = row('2026-08-26T01:00:00', group_uuid=self.U, code='agency-b')
        got = load_recovered.group_uuid_for(r, {self.U: 'agency-a:1'})
        assert got != self.U
        # identical to the path a row with no uuid at all takes
        assert got == load_recovered.group_uuid_for(dict(r, group_uuid=''))

    def test_two_minority_agencies_do_not_land_in_the_same_group(self):
        owners = {self.U: 'agency-a:1'}
        b = load_recovered.group_uuid_for(row('2026-08-26T01:00:00', group_uuid=self.U, code='agency-b'), owners)
        c = load_recovered.group_uuid_for(row('2026-08-26T01:00:00', group_uuid=self.U, code='agency-c'), owners)
        assert b != c

    def test_no_owner_map_leaves_every_uuid_alone(self):
        r = row('2026-08-26T01:00:00', group_uuid=self.U, code='agency-b')
        assert load_recovered.group_uuid_for(r) == self.U


class TestDbOwnersWin:
    """A uuid can be shared with a group that is already in the database rather than with another
    rendered agency -- the snapshot restore brings in 57,705 real groups, one of which a
    reconstructed agency also carries. Scanning the rendered files cannot see that, so what the database already holds is merged
    in on top, and it wins: those rows are real, already-written email.
    """

    U = '66666666-6666-6666-6666-666666666666'
    V = '77777777-7777-7777-7777-777777777777'

    def test_the_database_overrides_the_rendered_majority(self):
        merged = load_recovered.merge_db_owners({self.U: 'agency-a:1'}, {self.U: 'prime:30784'})
        assert merged == {self.U: 'prime:30784'}

    def test_a_uuid_only_the_database_knows_about_is_added(self):
        merged = load_recovered.merge_db_owners({}, {self.V: 'prime:30784'})
        assert merged == {self.V: 'prime:30784'}

    def test_rendered_ownership_survives_where_the_database_is_silent(self):
        merged = load_recovered.merge_db_owners({self.U: 'agency-a:1'}, {})
        assert merged == {self.U: 'agency-a:1'}

    def test_a_row_whose_uuid_the_database_gives_to_another_agency_is_synthesised(self):
        owners = load_recovered.merge_db_owners({}, {self.U: 'prime:30784'})
        r = row('2026-08-26T01:00:00', group_uuid=self.U, code='agency-a')
        assert load_recovered.group_uuid_for(r, owners) != self.U
