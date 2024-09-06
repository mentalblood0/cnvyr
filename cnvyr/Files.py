import base64
import dataclasses
import datetime
import hashlib
import pathlib
import zlib


@dataclasses.dataclass(frozen=True, kw_only=True)
class Files:
    root: pathlib.Path
    extension: str

    def __post_init__(self):
        if not self.extension.startswith("."):
            raise ValueError(f"expect extension starting with '.', got {self.extension}")

    def digest(self, data: bytes):
        return hashlib.sha512(data).digest()

    def compressed(self, data: bytes):
        return zlib.compress(data, level=zlib.Z_BEST_COMPRESSION)

    def decompressed(self, data: bytes):
        return zlib.decompress(data)

    def path(self, created: datetime.datetime, digest: bytes):
        return (
            self.root
            / str(created.year)
            / f"{created.month:02}"
            / f"{created.day:02}"
            / f"{created.hour:02}"
            / (f"{created.minute:02}_{created.second:02}_" + base64.b64encode(digest.rstrip(b"=")).decode("ascii"))
        ).with_suffix(self.extension + ".gz")

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
            raise ValueError(f"{path} digest: have {have}, got {digest}")
        return result
