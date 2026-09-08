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
            row('2025-09-02T13:43:20+00:00', group_uuid=u),  # different second
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
