import pathlib
import shutil

import pytest

from ..Files import Files

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture
def files():
    path = pathlib.Path("test_files")
    shutil.rmtree(path, ignore_errors=True)
    yield Files(root=path, extension=".txt")
    shutil.rmtree(path, ignore_errors=True)


@pytest.mark.asyncio
async def test_save_load(files: Files):
    data = b"lalala"
    created, digest = await files.save(data)
    assert await files.load(created, digest) == data
