import dataclasses
import datetime


@dataclasses.dataclass(frozen=True, kw_only=True)
class Item:
    id: int | None = dataclasses.field(default=None, compare=False)
    digest: bytes
    created: datetime.datetime
