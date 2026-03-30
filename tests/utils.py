import re


class RegexStr(str):
    """String subclass that compares equal to any string matching the given regex pattern."""

    def __eq__(self, other):
        if not isinstance(other, str):
            return NotImplemented
        return bool(re.search(str(self), other))

    def __hash__(self):
        return super().__hash__()

    def __repr__(self):
        return f'RegexStr({super().__repr__()})'
