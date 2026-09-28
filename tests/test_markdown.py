import pytest

from app.render.markdown import markdown

# Mirrors TC2's TutorCruncher/common/tests/test_markdown.py so the two suites can be diffed.


@pytest.mark.parametrize(
    'src, expected',
    [
        ('sentence with **bold** word.', '<p>sentence with <strong>bold</strong> word.</p>\n'),
        ('First.  \nSecond.', '<p>First.<br />\nSecond.</p>\n'),
        ('First.\nSecond.', '<p>First.<br />\nSecond.</p>\n'),
        ('First.\r\nSecond.', '<p>First.<br />\nSecond.</p>\n'),
        ('123\n456', '<p>123<br />\n456</p>\n'),
        ('First.Second.\n', '<p>First.Second.</p>\n'),
        ('First.\n\nSecond.', '<p>First.</p>\n<p>Second.</p>\n'),
        ('£100 compétences', '<p>£100 compétences</p>\n'),
    ],
)
def test_tc2_parity(src, expected):
    """These cases are asserted identically in TC2; the two must not drift."""
    assert markdown(src) == expected


def test_lone_dash_not_a_list():
    assert markdown('-') == '-'


def test_none_renders_empty():
    assert markdown(None) == ''


@pytest.mark.parametrize(
    'src, expected',
    [
        # Underscores inside words are not emphasis, which is what misaka's `no-intra-emphasis`
        # gave us and what every `centered_button(...)` macro call relies on.
        (
            'centered_button(Set Password | {{ confirm_link }})',
            '<p>centered_button(Set Password | {{ confirm_link }})</p>\n',
        ),
        ('we are_testing_emphasis', '<p>we are_testing_emphasis</p>\n'),
        ('snake_case_name', '<p>snake_case_name</p>\n'),
    ],
)
def test_no_intra_word_underscore_emphasis(src, expected):
    assert markdown(src) == expected


@pytest.mark.parametrize(
    'src',
    [
        'see <a href="https://x.com">here</a>',
        '<img src="https://x.com/a.png" alt="a">',
        'one<br>two',
    ],
)
def test_raw_html_passes_through(src):
    """misaka ran with escaping off; footers and templates carry their own HTML."""
    assert src in markdown(src)


def test_style_block_survives():
    src = '<style>\n  table { width: 100%; }\n</style>'
    assert markdown(src).startswith('<style>')


@pytest.mark.parametrize(
    'src',
    [
        'go to https://example.com now',
        'test email http://example.org/unsub.',
        'url slug: **https://example.com/abc**',
    ],
)
def test_bare_urls_are_not_autolinked(src):
    """TC2 enables mistune's `url` plugin; we deliberately do not. Bodies reach us after link
    shortening, and the plugin swallows a trailing '**' into the href."""
    assert '<a href' not in markdown(src)


def test_tables_render():
    src = 'header 1 | header 2\n-------- | --------\ntext 1   | text 2\n'
    out = markdown(src)
    assert '<table>' in out
    assert '<th>header 1</th>' in out
    # TC2 injects `class="w-auto as-markdown"` for its own Bootstrap styling; email must not.
    assert 'class=' not in out


def test_strikethrough():
    assert markdown('~~gone~~') == '<p><del>gone</del></p>\n'
