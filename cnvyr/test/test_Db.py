import dataclasses
import datetime
import enum
import json
import pathlib

import pytest

from ..Db import Db, Item

Operation = enum.Enum("Operation", ["test_save_load", "test_update__save", "test_update__update", "test_error_logging"])


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
    db.transaction(Operation.test_save_load, c)
    result = [*C.load_from(db, "select * from c")]
    assert len(result) == 1
    assert result[0] == c


def test_update(db: Db):
    c = C(digest=b"digest", created=datetime.datetime.now())
    db.transaction(Operation.test_update__save, c)
    created = [*C.load_from(db, "select * from c")][0]

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
    db.transaction(Operation.test_update__update, (created, updated))

    result = [*C.load_from(db, "select * from c")]
    assert len(result) == 1
    assert result[0] != created
    assert result[0] == updated


def test_error_logging(db: Db):
    operation = Operation.test_error_logging
    error_text = "some value is invalid"
    error_type = "ValueError"

    for i in range(3):
        with db.error_logging(operation):
            raise ValueError(error_text)
        result = db._connection.execute(
            "select operation, first, last, error_type, error_text, amount from cnvyr_errors"
        ).fetchall()
        assert len(result) == 1
        assert result[0][0] == operation.name
        if i == 0:
            assert result[0][1] == result[0][2]
        else:
            assert result[0][1] != result[0][2]
        assert result[0][3] == error_type
        assert result[0][4] == error_text
        assert result[0][5] == i + 1

    with db.error_logging(operation):
        pass
    result = db._connection.execute(
        "select operation, first, last, error_type, error_text, amount from cnvyr_errors"
    ).fetchall()
    assert not result
