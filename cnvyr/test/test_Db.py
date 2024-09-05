import dataclasses
import datetime
import enum
import json
import pathlib

import pytest

from ..Db import Db
from ..Item import Item


@pytest.fixture
def db():
    result = Db(**json.loads(pathlib.Path("credentials.json").read_text()))
    result.wipe()
    yield result
    result.wipe()


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


def test_save_load(db: Db):
    c = C(digest=b"digest", created=datetime.datetime.now())
    db.transaction(c)
    result = [*db.load("select * from c", C)]
    assert len(result) == 1
    assert result[0] == c


def test_update(db: Db):
    c = C(digest=b"digest", created=datetime.datetime.now())
    db.transaction(c)
    created = [*db.load("select * from c", C)][0]

    updated = dataclasses.replace(
        created,
        test_bool=False,
        test_string="lololo",
        test_int=321,
        test_float=321.321,
        test_bytes=b"lololo",
        test_datetime=datetime.datetime.now(),
        test_enum=E.B,
    )
    db.transaction((created, updated))

    result = [*db.load("select * from c", C)]
    assert len(result) == 1
    assert result[0] != created
    assert result[0] == updated
