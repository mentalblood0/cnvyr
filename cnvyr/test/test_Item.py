import dataclasses
import datetime
import enum

import pytest

from ..Item import Item

E = enum.Enum("E", ["A", "B"])


@dataclasses.dataclass(frozen=True, kw_only=True)
class C(Item):
    test_bool: bool = True
    test_string: str = "lalala"
    test_int: int = 123
    test_float: float = 123.123
    test_bytes: bytes = b"lalala"
    test_datetime: datetime.datetime = datetime.datetime.now()
    test_enum: E = E.A


def test_runtime_type_checking():
    for f in dataclasses.fields(C):
        if f.name in ("id", "created", "digest"):
            continue
        if f.type | bool == f.type:
            invalid_value = 0
        else:
            invalid_value = False
        with pytest.raises(ValueError):
            C(digest=b"digest", created=datetime.datetime.now(), **{f.name: invalid_value})
