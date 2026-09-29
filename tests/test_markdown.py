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


@pytest.mark.parametrize('src', ['-', '-\n', '-\r\n', '- ', ' - '])
def test_lone_dash_not_a_list(src):
    """Footers arrive from a textarea, so the trailing-newline forms matter."""
    assert markdown(src) == src


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


@pytest.mark.parametrize(
    'src, expected',
    [
        # misaka's `no-intra-emphasis` covered * as well as _; CommonMark only covers _, so
        # without our InlineParser these lose their asterisks and gain italics.
        ('text with 5*6=30 and 7*8', '<p>text with 5*6=30 and 7*8</p>\n'),
        ('price is 100**per hour**today', '<p>price is 100**per hour**today</p>\n'),
        ('x**2**y', '<p>x**2**y</p>\n'),
        ('costs 5*4 dollars', '<p>costs 5*4 dollars</p>\n'),
    ],
)
def test_no_intra_word_asterisk_emphasis(src, expected):
    assert markdown(src) == expected


@pytest.mark.parametrize(
    'src, expected',
    [
        # Only an *opening* marker is suppressed, so a closer against a word still pairs up.
        ('**20**th', '<p><strong>20</strong>th</p>\n'),
        ('**bold**word', '<p><strong>bold</strong>word</p>\n'),
        ('*italic* and **bold**', '<p><em>italic</em> and <strong>bold</strong></p>\n'),
        ('**Student(s)**: Alex', '<p><strong>Student(s)</strong>: Alex</p>\n'),
    ],
)
def test_emphasis_still_works_at_word_boundaries(src, expected):
    assert markdown(src) == expected


def test_task_lists_are_plain_list_items():
    """TC2 enables mistune's `task_lists`; we don't, because <input> is stripped by most email
    clients and misaka rendered the brackets literally."""
    assert markdown('- [ ] task') == '<ul>\n<li>[ ] task</li>\n</ul>\n'


def test_markdown_after_block_html_is_not_rendered():
    """Documents a known CommonMark behaviour misaka did not share: a block-level HTML tag opens
    an HTML block that runs to the next blank line, so markdown on the following line stays raw.
    TC2 renders this identically, so a footer previewed there matches the email sent here."""
    assert markdown('<div>\nx\n</div>\nCall **now**') == '<div>\nx\n</div>\nCall **now**\n\n'
    # A blank line closes the HTML block and the markdown renders as expected.
    assert '<strong>now</strong>' in markdown('<div>\nx\n</div>\n\nCall **now**')
