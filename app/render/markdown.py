from mistune import HTMLRenderer, InlineParser, Markdown, import_plugin

# Kept in lockstep with TC2's TutorCruncher/common/markdown.py: email bodies are authored and
# previewed in TC2 but rendered here, so both services must parse them with the same library,
# version and options. The one deliberate difference is TC2's 'url' plugin, which autolinks bare
# URLs -- morpheus renders bodies *after* link shortening, and enabling it both changes 66 of
# TC2's default email bodies and swallows a trailing '**' into the href ('slug: **{{ url }}**').
MD_PLUGINS = ['strikethrough', 'table', 'task_lists', 'speedup']


class EmailMarkdown(Markdown):
    def __call__(self, s: str | None) -> str:
        if s is None:
            s = '\n'
        elif s == '-':
            # The block parser reads a lone '-' as an empty list item; a footer of '-' should
            # stay a dash. Matches TC2.
            return s
        return self.parse(s)[0]


# escape=False mirrors TC2's `markdown_allow_html`, and is what misaka did: branch footers and
# email templates supply their own <style>, <table> and <img> markup, which must pass through.
markdown = EmailMarkdown(
    renderer=HTMLRenderer(escape=False),
    inline=InlineParser(hard_wrap=True),
    plugins=[import_plugin(n) for n in MD_PLUGINS],
)
