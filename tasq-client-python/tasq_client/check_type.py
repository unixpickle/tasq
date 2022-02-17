from dataclasses import dataclass
from typing import Any, Callable, List


@dataclass(frozen=True, eq=True)
class OptionalKey:
    """
    An object to use as a key in a type template to indicate that the field
    need not be present in a dict.
    """

    key: str


class CheckTypeException(Exception):
    """
    An error indicating that the type of an object does not match the expected
    template for the object's type.
    """

    pass


def check_type(template: Any, obj: Any):
    """
    Raise an exception if a given object does not match a type template.

    For example, a type template might be `str`, and `"hello"` would match the
    template whereas `1234` would not.

    Templates can include nested data structures. If a template is a dict, each
    key will map to a corresponding template for the value of that key. A key
    in a template must be present in the checked object, unless the key is
    wrapped in OptionalKey.

    If a template is a list, it should contain one object--the template for the
    elements of the list.

    The float template will accept both int and float types.
    """
    if isinstance(template, dict):
        if not isinstance(obj, dict):
            raise CheckTypeException(f"expected dict but got {type(obj)}")
        for k, v in template.items():
            if isinstance(k, OptionalKey):
                k = k.key
                if k not in obj:
                    continue
            if k not in obj:
                raise CheckTypeException(f"missing key: {k}")
            _wrap_check(f"value for key {k}", lambda: check_type(v, obj[k]))
    elif isinstance(template, list):
        if not isinstance(obj, list):
            raise CheckTypeException(f"expected list but got {type(obj)}")
        assert len(template) == 1
        value_template = template[0]
        for i, value in enumerate(obj):
            _wrap_check(
                f"value at index {i}", lambda: check_type(value_template, value)
            )
    elif template is float:
        if not isinstance(obj, int) and not isinstance(obj, float):
            raise CheckTypeException(
                f"expected type {template} to be float or int but got {type(obj)}"
            )
    else:
        if not isinstance(obj, template):
            raise CheckTypeException(f"expected type {template} but got {type(obj)}")


def _wrap_check(context: str, check_fn: Callable):
    try:
        check_fn()
    except CheckTypeException as exc:
        raise CheckTypeException(f"{context}: {str(exc)}")
