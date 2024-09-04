import base64
import dataclasses
import datetime
import hashlib
import pathlib
import zlib

from .Item import Item


@dataclasses.dataclass(frozen=True, kw_only=False)
class Files:
    root: pathlib.Path

    def digest(self, data):
        return hashlib.sha512(data).digest()

    def compressed(self, data):
        return zlib.compress(data, level=zlib.Z_BEST_COMPRESSION)

    def decompressed(self, data):
        return zlib.decompress(data)

    def path(self, created: datetime.datetime, digest: bytes):
        return (
            self.root
            / str(created.year)
            / str(created.month)
            / str(created.day)
            / str(created.hour)
            / (f"{created.minute}:{created.second}:" + base64.b64encode(digest.rstrip(b"=")).decode("ascii"))
        )

    def save(self, data: bytes):
        created = datetime.datetime.now(datetime.UTC)
        digest = self.digest(data)
        path = self.path(created, digest)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.compressed(data))
        return created, digest

    def load(self, created: datetime.datetime, digest: bytes):
        path = self.path(created, digest)
        result = self.decompressed(path.read_bytes())
        if (have := self.digest(result)) != digest:
            raise ValueError(f"for file created at {created}: have {have}, got {digest}")
        return result

    def load_for(self, item: Item):
        return self.load(item.created, item.digest)
