import pathlib
import shutil

import pytest

from ..Files import Files


@pytest.fixture
def files():
    path = pathlib.Path("test_files")
    shutil.rmtree(path, ignore_errors=True)
    yield Files(root=path, extension=".txt")
    shutil.rmtree(path, ignore_errors=True)


def test_save_load(files: Files):
    data = b"lalala"
    created, digest = files.save(data)
    assert files.load(created, digest) == data
