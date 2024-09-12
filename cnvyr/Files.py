import base64
import dataclasses
import datetime
import pathlib

import aiofile
import lz4.frame
import xxhash
from xxhash import xxh3_128_digest


@dataclasses.dataclass(frozen=True, kw_only=True)
class Files:
    root: pathlib.Path
    extension: str

    def __post_init__(self):
        if not self.extension.startswith("."):
            raise ValueError(f"expect extension starting with '.', got {self.extension}")

    def digest(self, data: bytes):
        return xxhash.xxh3_128_digest(data)

    def compressed(self, data: bytes):
        return lz4.frame.compress(data, compression_level=16)

    def decompressed(self, data: bytes):
        return lz4.frame.decompress(data)

    def path(self, created: datetime.datetime, digest: bytes):
        return (
            self.root
            / str(created.year)
            / f"{created.month:02}"
            / f"{created.day:02}"
            / f"{created.hour:02}"
            / (f"{created.minute:02}_{created.second:02}_" + base64.b64encode(digest.rstrip(b"=")).decode("ascii"))
        ).with_suffix(self.extension + ".gz")

    async def save(self, data: bytes):
        created = datetime.datetime.now(datetime.UTC)
        digest = self.digest(data)

        path = self.path(created, digest)
        path.parent.mkdir(parents=True, exist_ok=True)

        compressed = self.compressed(data)
        async with aiofile.async_open(path, mode="wb") as af:
            await af.write(compressed)

        return created, digest

    async def load(self, created: datetime.datetime, digest: bytes):
        path = self.path(created, digest)
        async with aiofile.async_open(path, mode="rb") as af:
            compressed = await af.read()
        result = self.decompressed(compressed)

        if (have := self.digest(result)) != digest:
            raise ValueError(f"{path} digest: have {have}, got {digest}")

        return result
