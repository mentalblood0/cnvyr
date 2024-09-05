import dataclasses
import datetime


@dataclasses.dataclass(frozen=True, kw_only=True)
class Item:
    id: int | None = dataclasses.field(default=None, compare=False)
    created: datetime.datetime
    digest: bytes

    def __post_init__(self):
        for f in dataclasses.fields(self):
            if f.type | type(value := getattr(self, f.name)) != f.type:
                raise ValueError(f"{self.__class__.__name__}.{f.name} expects value of type {f.type}, got {value}")
