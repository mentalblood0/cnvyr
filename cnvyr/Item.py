import dataclasses
import datetime


@dataclasses.dataclass(frozen=True, kw_only=True)
class Item:
    _id: int | None = dataclasses.field(default=None, compare=False)
    digest: bytes
    created: datetime.datetime

    @property
    def id(self):
        if not isinstance(self._id, int):
            raise ValueError
        return self._id
