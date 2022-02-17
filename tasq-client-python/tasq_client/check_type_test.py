import pytest

from .check_type import CheckTypeException, OptionalKey, check_type


@pytest.mark.parametrize(
    ["template", "value", "expected"],
    [
        (int, 3, True),
        (int, 3.0, False),
        (int, "hi", False),
        (float, 3, True),
        (float, 3.0, True),
        (float, "hi", False),
        ([str], [], True),
        ([str], ["hello"], True),
        ([str], ["hello", 3], False),
        ([str], 3, False),
        ([str], "hi", False),
        (dict(field=int), dict(), False),
        (dict(field=int), dict(field=3), True),
        (dict(field=int), dict(field=3, other=3), True),
        ({"field": int, OptionalKey("other"): str}, dict(field=3), True),
        ({"field": int, OptionalKey("other"): str}, dict(field=3, other="hi"), True),
        ({"field": int, OptionalKey("other"): str}, dict(field=3, other=3), False),
        (dict(field=dict(baz=int)), dict(field=3), False),
        (dict(field=dict(baz=int)), dict(field=dict(baz="hi")), False),
        (dict(field=dict(baz=int)), dict(field=dict(baz=3)), True),
    ],
)
def test_check_type(template, value, expected):
    if expected:
        check_type(template, value)
    else:
        try:
            check_type(template, value)
            assert False, f"template {template} should not match {value}"
        except CheckTypeException:
            pass

    # Everything should be an object.
    check_type(object, value)
