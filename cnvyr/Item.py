import dataclasses
import datetime


@dataclasses.dataclass(kw_only=True)
class Item:
    _id: int | None = None
    _digest: bytes | None = None
    _created: datetime.datetime | None = None

    @property
    def id(self):
        if not isinstance(self._id, int):
            raise ValueError
        return self._id

    @property
    def digest(self):
        if not isinstance(self._digest, bytes):
            raise ValueError
        return self._digest

    @property
    def created(self):
        if not isinstance(self._created, datetime.datetime):
            raise ValueError
        return self._created
