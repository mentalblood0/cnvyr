import asyncio
import functools
import pathlib
import shutil

import pytest
import pytest_asyncio

from ..Files import Files

pytest_plugins = ("pytest_asyncio", "pytest_benchmark")


@pytest.fixture
def files():
    path = pathlib.Path("/mnt/tmpfs")
    shutil.rmtree(path, ignore_errors=True)
    yield Files(root=path, extension=".txt")
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
@functools.cache
def data():
    return (pathlib.Path(__file__).parent.parent.parent / "LICENSE").read_bytes()


@pytest.mark.asyncio
async def test_save_load(files: Files, data: bytes):
    created, digest = await files.save(data)
    assert await files.load(created, digest) == data


@pytest_asyncio.fixture
async def aio_benchmark(benchmark):
    async def run_async_coroutine(func, *args, **kwargs):
        return await func(*args, **kwargs)

    def _wrapper(func, *args, **kwargs):
        if asyncio.iscoroutinefunction(func):

            @benchmark
            def _():
                future = asyncio.ensure_future(run_async_coroutine(func, *args, **kwargs))
                return asyncio.get_event_loop().run_until_complete(future)

        else:
            benchmark(func, *args, **kwargs)

    return _wrapper


def test_benchmark_save_load(aio_benchmark, files: Files, data: bytes):
    async def f(files: Files, data: bytes):
        created, digest = await files.save(data)
        assert await files.load(created, digest) == data

    aio_benchmark(f, files, data)
