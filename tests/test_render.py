import pytest

from src.render.main import MessageTooLong, SmsLength, sms_length


def idfn(v):
    if isinstance(v, str):
        return f'{v[:20]}:{len(v)}'


@pytest.mark.parametrize(
    'msg, length, sms_count',
    [
        ('123', 3, 1),
        ('123\n456', 8, 1),
        ('123😀', 3, 1),
        ('123®', 4, 1),
        ('123{', 5, 1),
        ('{}', 4, 1),
        ('a' * 160, 160, 1),
        ('b' * 161, 161, 2),
        ('c' * 306, 306, 2),
        ('d' * 307, 307, 3),
        ('e' * 1377, 1377, 9),
        ('{' * 100, 200, 2),
    ],
    ids=idfn,
)
def test_sms_lengths(msg, length, sms_count):
    assert sms_length(msg) == SmsLength(length, sms_count)


def test_sms_too_long():
    with pytest.raises(MessageTooLong) as exc_info:
        sms_length('x' * 1378)
    assert exc_info.value.args[0] == 'message length 1378 exceeds maximum multi-part SMS length 1377'
