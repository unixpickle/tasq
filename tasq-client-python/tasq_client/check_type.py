from dataclasses import dataclass
from typing import Any, Callable, List


@dataclass
class OptionalKey:
    key: str


@dataclass
class TemplateUnion:
    templates: List[Any]


class CheckTypeException(Exception):
    pass


def check_type(template: Any, obj: Any):
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
                f"value at index {i}", lambda: _wrap_check(value_template, value)
            )
    elif isinstance(template, TemplateUnion):
        for x in template.templates:
            try:
                check_type(x, obj)
                return
            except CheckTypeException:
                pass
        raise CheckTypeException(f"unexpected type {type(obj)}")
    else:
        if not isinstance(obj, template):
            raise CheckTypeException(f"expected type {template} but got {obj}")


def _wrap_check(context: str, check_fn: Callable):
    try:
        check_fn()
    except CheckTypeException as exc:
        return CheckTypeException(f"{context}: {str(exc)}")
