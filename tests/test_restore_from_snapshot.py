"""Unit tests for the snapshot restore's pure helpers (scripts/restore_from_snapshot.py).

Companion to test_load_recovered.py, and the same rule applies: the script is run by hand against a
--dsn and is never imported by the app, so it is loaded here by path, and only the logic that decides
*what* gets written is covered. A mistake in these three helpers files one agency's mail under
another, corrupts every body that contains a tab or a newline, or restores primary keys the database
is about to hand out again.

No real agency codes, addresses or ids appear here; the fixtures are invented.
"""

import importlib.util
from pathlib import Path

_SCRIPT = Path(__file__).parent.parent / 'scripts' / 'restore_from_snapshot.py'
_spec = importlib.util.spec_from_file_location('restore_from_snapshot', _SCRIPT)
restore = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(restore)


class TestResolveRemap:
    """company_id is the only field the restore rewrites. The snapshot's companies rows were deleted
    and re-created by later sends under the same codes with different ids, so the mapping is by code
    -- and a wrong mapping silently files one agency's email history under another agency."""

    def test_maps_snapshot_ids_to_current_ids_by_code(self):
        mapping, problems = restore.resolve_remap(
            {11: 'agency-a:1', 12: 'agency-a:2', 13: 'agency-b:3'},
            [11, 12, 13],
            {'agency-a:1': 900, 'agency-a:2': 901, 'agency-b:3': 902, 'someone-else:9': 903},
        )
        assert mapping == {11: 900, 12: 901, 13: 902}
        assert problems == []

    def test_ids_are_not_carried_over_when_they_happen_to_match(self):
        # The snapshot id and the current id for a code are unrelated numbers. A remap that quietly
        # kept the old id would look correct here only by coincidence.
        mapping, _ = restore.resolve_remap({11: 'agency-a:1'}, [11], {'agency-a:1': 11})
        assert mapping == {11: 11}
        mapping, _ = restore.resolve_remap({11: 'agency-a:1'}, [11], {'agency-a:1': 4242})
        assert mapping == {11: 4242}

    def test_code_missing_from_the_target_is_a_problem_not_a_silent_skip(self):
        mapping, problems = restore.resolve_remap(
            {11: 'agency-a:1', 12: 'agency-a:2'}, [11, 12], {'agency-a:1': 900}
        )
        assert mapping == {11: 900}
        assert len(problems) == 1
        assert 'agency-a:2' in problems[0]

    def test_id_missing_from_the_snapshot_companies_table_is_a_problem(self):
        _, problems = restore.resolve_remap({11: 'agency-a:1'}, [11, 99], {'agency-a:1': 900})
        assert len(problems) == 1
        assert '99' in problems[0]

    def test_only_the_ids_actually_used_need_resolving(self):
        # A branch that exists but sent nothing has no rows to remap, so its absence from the target
        # must not block the load.
        mapping, problems = restore.resolve_remap(
            {11: 'agency-a:1', 12: 'agency-a:never-sent'}, [11], {'agency-a:1': 900}
        )
        assert mapping == {11: 900}
        assert problems == []


class TestSequenceProblems:
    """Restoring original primary keys is only safe while every one sits below its sequence. If one
    reaches it, the database is about to hand that id out again to a different row."""

    def test_ids_below_the_sequence_are_fine(self):
        assert restore.sequence_problems({'messages': 100}, {'messages': 101}) == []

    def test_id_equal_to_the_sequence_is_a_problem(self):
        # last_value is the id most recently handed out, so an equal id is already taken.
        assert len(restore.sequence_problems({'messages': 100}, {'messages': 100})) == 1

    def test_id_past_the_sequence_is_a_problem(self):
        problems = restore.sequence_problems({'messages': 500}, {'messages': 100})
        assert len(problems) == 1
        assert 'messages' in problems[0]

    def test_reports_every_offending_table_not_just_the_first(self):
        problems = restore.sequence_problems(
            {'message_groups': 1, 'messages': 500, 'events': 900, 'links': 2},
            {'message_groups': 10, 'messages': 100, 'events': 90, 'links': 20},
        )
        assert len(problems) == 2
        assert {p.split(':')[0] for p in problems} == {'messages', 'events'}


class TestPgText:
    """Values are handed to COPY as text. Email bodies are HTML full of newlines, and a tag can hold
    a tab, so an unescaped character ends the row or the field early and shifts every column after
    it -- which COPY accepts happily until the types stop lining up."""

    def test_none_becomes_the_null_marker(self):
        # \\N is COPY's NULL, distinct from the two-character string 'N' or an empty field.
        assert restore.pg_text(None) == '\\N'

    def test_booleans_use_the_postgres_short_forms(self):
        assert restore.pg_text(True) == 't'
        assert restore.pg_text(False) == 'f'

    def test_empty_string_is_not_null(self):
        assert restore.pg_text('') == ''

    def test_newline_would_otherwise_end_the_row(self):
        assert restore.pg_text('line one\nline two') == 'line one\\nline two'

    def test_tab_would_otherwise_end_the_field(self):
        assert restore.pg_text('a\tb') == 'a\\tb'

    def test_carriage_return_is_escaped(self):
        assert restore.pg_text('a\r\nb') == 'a\\r\\nb'

    def test_backslash_is_escaped_first(self):
        # Escaping the backslash after the others would turn an escape sequence we just wrote into a
        # literal backslash plus 'n', so ordering is the whole game here.
        assert restore.pg_text('a\\nb') == 'a\\\\nb'
        assert restore.pg_text('back\\slash\nnewline') == 'back\\\\slash\\nnewline'

    def test_html_body_survives_intact(self):
        body = '<p>Hi</p>\n<a href="https://example.com/x?a=1&b=2">Link</a>\n'
        assert restore.pg_text(body) == body.replace('\n', '\\n')

    def test_numbers_and_arrays_are_stringified(self):
        assert restore.pg_text(0) == '0'
        assert restore.pg_text(1.5) == '1.5'
        assert restore.pg_text('{tag-one,tag-two}') == '{tag-one,tag-two}'


class TestConstants:
    def test_tables_are_in_foreign_key_order(self):
        # Each table points at the one before it, so any other order fails on insert.
        assert restore.TABLES == ['message_groups', 'messages', 'events', 'links']

    def test_vector_is_never_inserted(self):
        # The create_tsvector BEFORE INSERT trigger rebuilds it, as it does for a normal send.
        assert 'vector' in restore.SKIP_COLUMNS
