import dataclasses
import datetime
import json
import pathlib

import pytest

from ..Db import Db
from ..Item import Item


@pytest.fixture
def db():
    result = Db(**json.loads(pathlib.Path("credentials.json").read_text()))
    yield result
    result.wipe()


@dataclasses.dataclass(frozen=True, kw_only=True)
class C(Item):
    test_bool: bool = True
    test_string: str = "lalala"
    test_int: int = 1234
    test_float: float = 1234.1234
    test_bytes: bytes = b"lalala"
    test_datetime: datetime.datetime = datetime.datetime.now()


def test_save_load(db: Db):
    c = C(digest=b"digest", created=datetime.datetime.now())
    db.save(c)
    result = [*db.load("select * from c", C)]
    assert len(result) == 1
    assert result[0] == c
