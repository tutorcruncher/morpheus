"""Unit tests for the remote recovery driver (scripts/recover_from_s3.py).

The driver's job is to fetch the two archives from S3 and then run the two loaders that were
rehearsed by hand -- it deliberately reimplements none of their logic, so what is worth covering here
is what it *decides*: which agencies run, in what order, under what batch id, and with which bounds.
A mistake in any of those either skips an agency's mail or loads a bounded slice unbounded.

No real agency codes, addresses or ids appear here beyond the campaign order itself, which is public
in the runbook.
"""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parent.parent / 'scripts' / 'recover_from_s3.py'
_spec = importlib.util.spec_from_file_location('recover_from_s3', _SCRIPT)
recover = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(recover)


class TestCampaignOrder:
    """Smallest agency first, so the one-row agency is a live smoke test before the 87,909-row one."""

    def test_all_twenty_five_agencies_are_listed(self):
        assert len(recover.AGENCIES) == 25
        assert len(set(recover.AGENCIES)) == 25

    def test_the_two_snapshot_agencies_are_not_in_the_list(self):
        # They are Route A. Loading their whole rendered history here would give them reconstructed
        # bodies alongside the real ones the snapshot restores.
        assert 'capital-tuition-group' not in recover.AGENCIES
        assert 'prime-uk-education-ltd' not in recover.AGENCIES

    def test_only_filter_picks_a_single_agency(self):
        assert recover.agencies_to_run('tuition360') == ['tuition360']

    def test_only_filter_rejects_an_agency_that_is_not_in_the_campaign(self):
        with pytest.raises(SystemExit):
            recover.agencies_to_run('capital-tuition-group')

    def test_no_filter_runs_the_whole_campaign_in_order(self):
        assert recover.agencies_to_run(None) == recover.AGENCIES


class TestBatchId:
    def test_batch_id_matches_the_runbook_shape(self):
        assert recover.batch_id('tuition360', '20260910') == 'recover-tuition360-20260910'

    def test_the_date_is_what_makes_a_rerun_distinguishable(self):
        assert recover.batch_id('tuition360', '20260101') != recover.batch_id('tuition360', '20260910')


class TestTails:
    """Capital and Prime kept sending between the 01:06 snapshot and their own deletion. Only that
    window comes from Route B -- a whole-day load would also insert what they sent afterwards, which
    is still live in production."""

    def test_both_tails_are_defined(self):
        assert {t['agency'] for t in recover.TAILS} == {'capital-tuition-group', 'prime-uk-education-ltd'}

    def test_each_tail_ends_at_its_own_deletion_time(self):
        by_agency = {t['agency']: t for t in recover.TAILS}
        assert by_agency['capital-tuition-group']['end_ts'] == '2026-08-26T19:48:37'
        assert by_agency['prime-uk-education-ltd']['end_ts'] == '2026-08-26T02:08:31'

    def test_both_tails_start_at_the_snapshot(self):
        assert all(t['start_ts'] == '2026-08-26T01:06:00' for t in recover.TAILS)

    def test_both_tails_are_bounded_to_the_single_day(self):
        assert all(t['day'] == '2026-08-26' for t in recover.TAILS)


class TestCommands:
    DSN = 'postgresql://user@host/db'

    def test_restore_command_points_at_the_extracted_layout(self):
        cmd = recover.restore_command(self.DSN, Path('/tmp/x'), dry_run=False)
        assert '--input' in cmd
        assert cmd[cmd.index('--input') + 1] == '/tmp/x/route_a_filtered'
        assert '--dry-run' not in cmd

    def test_dry_run_is_passed_through_to_the_loader(self):
        assert '--dry-run' in recover.restore_command(self.DSN, Path('/tmp/x'), dry_run=True)
        assert '--dry-run' in recover.load_command(self.DSN, Path('/tmp/x'), 'a', 'b', dry_run=True)

    def test_load_command_has_no_bounds_for_an_ordinary_agency(self):
        cmd = recover.load_command(self.DSN, Path('/tmp/x'), 'tuition360', 'batch-1', dry_run=False)
        assert '--agency' in cmd and cmd[cmd.index('--agency') + 1] == 'tuition360'
        assert '--start-ts' not in cmd and '--end-ts' not in cmd and '--start' not in cmd

    def test_load_command_carries_every_bound_for_a_tail(self):
        tail = recover.TAILS[0]
        cmd = recover.load_command(self.DSN, Path('/tmp/x'), tail['agency'], 'capital-tail', dry_run=False, tail=tail)
        for flag, value in [
            ('--start', tail['day']),
            ('--end', tail['day']),
            ('--start-ts', tail['start_ts']),
            ('--end-ts', tail['end_ts']),
        ]:
            assert flag in cmd and cmd[cmd.index(flag) + 1] == value

    def test_the_rendered_folder_is_where_the_archive_puts_it(self):
        cmd = recover.load_command(self.DSN, Path('/tmp/x'), 'tuition360', 'b', dry_run=False)
        assert cmd[cmd.index('--input') + 1] == '/tmp/x/rendered'

    def test_no_loader_is_ever_given_as_code_or_method(self):
        # --as-code re-homes mail onto another agency and --method makes it invisible to production
        # TC2. Both are local-testing flags and must never reach a real run.
        cmd = recover.load_command(self.DSN, Path('/tmp/x'), 'tuition360', 'b', dry_run=False)
        assert '--as-code' not in cmd and '--method' not in cmd


class TestDatabaseUrl:
    """Heroku hands out postgres:// URLs, which psycopg2 accepts but SQLAlchemy does not; normalise
    once here so the driver behaves the same wherever it runs."""

    def test_heroku_style_scheme_is_normalised(self, monkeypatch):
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
