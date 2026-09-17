"""Unit tests for the remote recovery driver (scripts/recover_from_s3.py).

The driver fetches the archives and then runs the two loaders that were rehearsed by hand -- it
reimplements none of their logic, so what is worth covering is what it *decides*: which agencies run,
under what batch id, and with which bounds. A mistake in any of those either skips an agency's mail
or loads a bounded window unbounded.

Which agencies exist is downloaded, not compiled in, so no customer names appear here either. The
fixtures are invented, and the rule the real list has to obey is tested on them.
"""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parent.parent / 'scripts' / 'recover_from_s3.py'
_spec = importlib.util.spec_from_file_location('recover_from_s3', _SCRIPT)
recover = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(recover)


def doc(codes=('agency-a', 'agency-b'), partial=()):
    return {
        'agencies': {'codes': list(codes)},
        'partial_agencies': {'entries': list(partial)},
    }


WINDOW = {
    'agency': 'agency-z',
    'day': '2026-08-26',
    'start_ts': '2026-08-26T01:06:00',
    'end_ts': '2026-08-26T19:48:37',
    'expected': 132,
}


class TestParseAgencyList:
    def test_a_well_formed_list_is_returned_as_given(self):
        agencies, partial = recover.parse_agency_list(doc(partial=[WINDOW]))
        assert agencies == ['agency-a', 'agency-b']
        assert partial == [WINDOW]

    def test_order_is_preserved_because_it_is_the_load_order(self):
        # Smallest agency first, so the smallest load is a live smoke test before the largest.
        agencies, _ = recover.parse_agency_list(doc(codes=('c', 'a', 'b')))
        assert agencies == ['c', 'a', 'b']

    def test_an_agency_in_both_lists_stops_the_run(self):
        # The one rule that matters: an agency whose real email was restored from the snapshot must
        # never also have its whole reconstructed history loaded on top.
        with pytest.raises(SystemExit) as exc:
            recover.parse_agency_list(doc(codes=('agency-a', 'agency-z'), partial=[WINDOW]))
        assert 'agency-z' in str(exc.value) or 'both' in str(exc.value)

    def test_a_bounded_window_missing_its_end_is_refused(self):
        # Without an end timestamp the load would run to the end of the day and re-insert email the
        # agency sent after it was deleted, which is still live.
        with pytest.raises(SystemExit):
            recover.parse_agency_list(doc(partial=[{**WINDOW, 'end_ts': ''}]))

    @pytest.mark.parametrize('field', ['agency', 'day', 'start_ts', 'end_ts'])
    def test_every_bound_is_required(self, field):
        with pytest.raises(SystemExit):
            recover.parse_agency_list(doc(partial=[{k: v for k, v in WINDOW.items() if k != field}]))

    def test_duplicates_are_refused(self):
        with pytest.raises(SystemExit):
            recover.parse_agency_list(doc(codes=('agency-a', 'agency-a')))

    def test_an_empty_list_is_refused_rather_than_loading_nothing_quietly(self):
        with pytest.raises(SystemExit):
            recover.parse_agency_list(doc(codes=()))

    def test_a_list_with_no_partial_agencies_is_fine(self):
        agencies, partial = recover.parse_agency_list({'agencies': {'codes': ['agency-a']}})
        assert agencies == ['agency-a'] and partial == []


class TestAgenciesToRun:
    def test_no_filter_runs_them_all_in_order(self):
        assert recover.agencies_to_run(['a', 'b', 'c'], None) == ['a', 'b', 'c']

    def test_only_picks_a_single_agency(self):
        assert recover.agencies_to_run(['a', 'b'], 'b') == ['b']

    def test_only_rejects_an_agency_that_is_not_in_the_list(self):
        with pytest.raises(SystemExit):
            recover.agencies_to_run(['a', 'b'], 'agency-z')


class TestBatchId:
    def test_batch_id_matches_the_runbook_shape(self):
        assert recover.batch_id('agency-a', '20260910') == 'recover-agency-a-20260910'

    def test_the_date_is_what_makes_a_rerun_distinguishable(self):
        assert recover.batch_id('agency-a', '20260101') != recover.batch_id('agency-a', '20260910')


class TestCommands:
    DSN = 'postgresql://user@host/db'

    def test_restore_command_points_at_the_extracted_layout(self):
        cmd = recover.restore_command(self.DSN, Path('/tmp/x'), dry_run=False)
        assert cmd[cmd.index('--input') + 1] == '/tmp/x/route_a_filtered'
        assert '--dry-run' not in cmd

    def test_dry_run_is_passed_through_to_the_loader(self):
        assert '--dry-run' in recover.restore_command(self.DSN, Path('/tmp/x'), dry_run=True)
        assert '--dry-run' in recover.load_command(self.DSN, Path('/tmp/x'), 'a', 'b', dry_run=True)

    def test_an_ordinary_agency_gets_no_bounds(self):
        cmd = recover.load_command(self.DSN, Path('/tmp/x'), 'agency-a', 'batch-1', dry_run=False)
        assert cmd[cmd.index('--agency') + 1] == 'agency-a'
        assert '--start-ts' not in cmd and '--end-ts' not in cmd and '--start' not in cmd

    def test_a_bounded_window_carries_every_bound(self):
        cmd = recover.load_command(self.DSN, Path('/tmp/x'), WINDOW['agency'], 'b', dry_run=False, tail=WINDOW)
        for flag, value in [
            ('--start', WINDOW['day']),
            ('--end', WINDOW['day']),
            ('--start-ts', WINDOW['start_ts']),
            ('--end-ts', WINDOW['end_ts']),
        ]:
            assert cmd[cmd.index(flag) + 1] == value

    def test_the_rendered_folder_is_where_the_archive_puts_it(self):
        cmd = recover.load_command(self.DSN, Path('/tmp/x'), 'agency-a', 'b', dry_run=False)
        assert cmd[cmd.index('--input') + 1] == '/tmp/x/rendered'

    def test_no_loader_is_ever_given_as_code_or_method(self):
        # --as-code re-homes mail onto another agency and --method makes it invisible to production
        # TC2. Both are local-testing flags and must never reach a real run.
        cmd = recover.load_command(self.DSN, Path('/tmp/x'), 'agency-a', 'b', dry_run=False)
        assert '--as-code' not in cmd and '--method' not in cmd


class TestDatabaseUrl:
    """The platform hands out postgres:// URLs, which psycopg2 accepts but SQLAlchemy does not;
    normalise once so the driver behaves the same wherever it runs."""

    def test_legacy_scheme_is_normalised(self, monkeypatch):
        monkeypatch.setenv('DATABASE_URL', 'postgres://u:p@h:5432/d')
        assert recover.database_url().startswith('postgresql://')

    def test_an_already_correct_url_is_left_alone(self, monkeypatch):
        monkeypatch.setenv('DATABASE_URL', 'postgresql://u:p@h:5432/d')
        assert recover.database_url() == 'postgresql://u:p@h:5432/d'

    def test_a_missing_url_stops_rather_than_guessing_a_local_database(self, monkeypatch):
        monkeypatch.delenv('DATABASE_URL', raising=False)
        with pytest.raises(SystemExit):
            recover.database_url()


class TestPhases:
    def test_each_phase_names_the_archives_it_needs(self):
        assert recover.archives_for('route-a') == ['route_a.tar.gz']
        assert recover.archives_for('route-b') == ['route_b.tar.gz']
        assert recover.archives_for('tails') == ['route_b.tar.gz']
        assert recover.archives_for('all') == ['route_a.tar.gz', 'route_b.tar.gz']

    def test_an_unknown_phase_is_refused(self):
        with pytest.raises(SystemExit):
            recover.archives_for('everything')
