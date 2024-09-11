import asyncio
import dataclasses
import datetime
import enum
import json
import pathlib
import platform

import pytest
import pytest_asyncio

from ..Db import Db, Item

if platform.system() == "Windows":
    import asyncio

    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

Operation = enum.Enum("Operation", ["test_save_load", "test_update__save", "test_update__update", "test_error_logging"])


@pytest_asyncio.fixture
async def db():
    result = Db(**json.loads(pathlib.Path("credentials.json").read_text()))
    await result.init()
    await result.wipe()
    yield result
    await result.wipe()


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


@pytest.mark.asyncio
async def test_save_load(db: Db):
    c = C(digest=b"digest", created=datetime.datetime.now())
    await db.transaction(Operation.test_save_load, c)
    result = [r async for r in C.load_from(db, "select * from c")]
    assert len(result) == 1
    assert result[0] == c


@pytest.mark.asyncio
async def test_update(db: Db):
    c = C(digest=b"digest", created=datetime.datetime.now())
    await db.transaction(Operation.test_update__save, c)
    created = [r async for r in C.load_from(db, "select * from c")][0]

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
    await db.transaction(Operation.test_update__update, (created, updated))

    result = [r async for r in C.load_from(db, "select * from c")]
    assert len(result) == 1
    assert result[0] != created
    assert result[0] == updated


@pytest.mark.asyncio
async def test_error_logging(db: Db):
    operation = Operation.test_error_logging
    error_text = "some value is invalid"
    error_type = "ValueError"

    for i in range(3):
        async with db.error_logging(operation):
            raise ValueError(error_text)
        result = await (
            await db._connection.execute(
                "select operation, first, last, error_type, error_text, amount from cnvyr_errors"
            )
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

    async with db.error_logging(operation):
        pass
    result = await (
        await db._connection.execute("select operation, first, last, error_type, error_text, amount from cnvyr_errors")
    ).fetchall()
    assert not result
