import re
from typing import Any, Match

from mistune import HTMLRenderer, InlineParser, Markdown, import_plugin
from mistune.core import InlineState

# Kept in lockstep with TC2's TutorCruncher/common/markdown.py: email bodies are authored and
# previewed in TC2 but rendered here, so both services must parse them with the same library,
# version and options. Two of TC2's plugins are deliberately left out:
#   'url'        autolinks bare URLs. We render bodies *after* link shortening, and it changes 66
#                of TC2's default email bodies and swallows a trailing '**' into the href, turning
#                'slug: **{{ url }}**' into <a href="...**">.
#   'task_lists' emits <input type="checkbox">, which most email clients strip. misaka left
#                '- [ ] x' as a plain list item.
# 'speedup' is dropped because it is a no-op shim in mistune 3.x ("installing this plugin
# intentionally leaves the Markdown instance unchanged").
MD_PLUGINS = ['strikethrough', 'table']


class NoIntraEmphasisInlineParser(InlineParser):
    """Suppress emphasis markers that sit inside a word, as misaka's `no-intra-emphasis` did.

    CommonMark already refuses intra-word `_`, but allows intra-word `*`, so without this
    `5*6=30 and 7*8` renders as `5<em>6=30 and 7</em>8` and loses the asterisks.

    Like Hoedown, only a marker that would *open* emphasis is suppressed: a closing marker may
    sit against a word, so `**20**th` still bolds.
    """

    def parse_emphasis(self, m: Match[str], state: InlineState) -> int:
        before = state.src[m.start() - 1] if m.start() else ''
        after = state.src[m.end()] if m.end() < len(state.src) else ''
        if before.isalnum() and after.isalnum() and not self._opened_earlier(m.group(0)[0], state, m.start()):
            # parse_emphasis=False marks the token so the finalizer never pairs it up.
            self.process_text(m.group(0), state, parse_emphasis=False)
            return m.end()
        return super().parse_emphasis(m, state)

    @staticmethod
    def _opened_earlier(char: str, state: InlineState, pos: int) -> bool:
        """Whether an earlier run of `char` could have opened emphasis this marker would close."""
        for run in re.finditer(re.escape(char) + '{1,3}', state.src[:pos]):
            before = state.src[run.start() - 1] if run.start() else ''
            after = state.src[run.end()] if run.end() < len(state.src) else ''
            if not before.isalnum() and after and not after.isspace():
                return True
        return False


class EmailMarkdown(Markdown):
    def __call__(self, s: str | None) -> str | list[dict[str, Any]]:
        if s is None:
            # mistune handles this too, but its signature says `str`, so narrow it here.
            s = '\n'
        elif s.strip() == '-':
            # The block parser reads a lone '-' as an empty list item; a footer of '-' should
            # stay a dash. strip() because it reaches us from a textarea, so '-\n' is common.
            return s
        return super().__call__(s)


# escape=False mirrors TC2's `markdown_allow_html`, and is what misaka did: branch footers and
# email templates supply their own <style>, <table> and <img> markup, which must pass through.
markdown = EmailMarkdown(
    renderer=HTMLRenderer(escape=False),
    inline=NoIntraEmphasisInlineParser(hard_wrap=True),
    plugins=[import_plugin(n) for n in MD_PLUGINS],
)
