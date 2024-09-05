import contextlib
import dataclasses
import datetime
import enum
import typing

import psycopg
import psycopg.rows
import psycopg.sql

from .Item import Item


@dataclasses.dataclass(frozen=False, kw_only=True)
class Db:
    user: str
    password: str
    name: str
    host: str = "127.0.0.1"
    port: int = 5432

    def __post_init__(self):
        self.connection = self.new_connection

    @property
    def new_connection(self):
        return psycopg.connect(
            f"host={self.host} port={self.port} dbname={self.name} user={self.user} password={self.password}"
        )

    def table_name(self, item: Item):
        return type(item).__name__.lower()

    def create_table(self, c: Item):
        t_name = self.table_name(c)
        ct_query = f"create table if not exists {t_name}"
        fields = []
        ci_queries = []

        for f in dataclasses.fields(c):
            f_name = f.name.lower()
            if f_name == "id":
                fields.append(f"{f_name} bigserial not null")
            else:

                ci_queries.append(f"create index if not exists {t_name}_{f_name} on {t_name}({f_name})")
                field = [f_name]

                if f.type | bool == f.type:
                    field.append("boolean")
                elif f.type | str == f.type:
                    field.append("text")
                elif f.type | int == f.type:
                    field.append("bigint")
                elif f.type | float == f.type:
                    field.append("double precision")
                elif f.type | bytes == f.type:
                    field.append("bytea")
                elif f.type | datetime.datetime == f.type:
                    field.append("timestamp")
                elif isinstance(f.type, type) and issubclass(f.type, enum.Enum):
                    field.append("smallint")
                else:
                    raise ValueError(f.type)

                if typing.Union[f.type, None] != f.type:
                    field.append("not null")

                fields.append(" ".join(field))

        ct_query += f"({', '.join(fields)})"

        with self.connection.cursor() as cursor:
            for q in [ct_query] + ci_queries:
                cursor.execute(q)

    def wipe(self):
        with self.connection.cursor() as cursor:
            cursor.execute("drop schema public cascade")
            cursor.execute("create schema public")
            cursor.execute("grant all on schema public to postgres")
            cursor.execute("grant all on schema public to public")

    def _create(self, item: Item, cursor: psycopg.Cursor):
        self.create_table(item)
        query = f"insert into {self.table_name(item)}"
        fields = self.asdict(item)
        del fields["id"]
        query += "(" + ", ".join(fields) + ") values (" + ", ".join(f"%({k})s" for k in fields) + ")"
        cursor.execute(query, fields)

    def asdict(self, item: Item):
        return {k: v.value if isinstance(v, enum.Enum) else v for k, v in dataclasses.asdict(item).items()}

    def _update(self, old: Item, new: Item, cursor: psycopg.Cursor):
        t_name = self.table_name(old)
        if t_name != (t_name_new := self.table_name(new)):
            raise ValueError(f"old item table name {t_name} != {t_name_new}")
        if old.id != new.id:
            raise ValueError(f"old item id {old.id} != {new.id}")

        query = f"update {t_name} set "
        d_old = self.asdict(old)
        d_new = self.asdict(new)

        diff = dict(set(d_new.items()) - set(d_old.items()))
        if not diff:
            return
        for k, v in diff.items():
            if k in Item.__dataclass_fields__:
                raise ValueError(f"attempt to change constant field: ({k}, {v})")
        query += ", ".join(f"{k}=%({k})s" for k in diff)

        rdiff = dict(set(d_old.items()) - set(d_new.items()))
        query += " where " + " and ".join(f"{k}=%(_{k})s" for k in rdiff.keys())

        cursor.execute(query, diff | {f"_{k}": v for k, v in rdiff.items()})

    def transaction(self, *actions: Item | tuple[Item, Item]):
        with self.connection.cursor() as cursor:
            for a in actions:
                if isinstance(a, Item):
                    self._create(a, cursor)
                elif isinstance(a, tuple) and len(a) == 2 and isinstance(a[0], Item) and isinstance(a[1], Item):
                    self._update(*a, cursor)
                else:
                    raise ValueError(a)

    def load(self, query: str, t: type[Item]):
        with self.connection.cursor(row_factory=psycopg.rows.class_row(t)) as cursor:
            for r in cursor.execute(query):
                enum_updates = {}
                for f in dataclasses.fields(r):
                    if (
                        isinstance(f.type, type)
                        and issubclass(f.type, enum.Enum)
                        and isinstance((v := getattr(r, f.name)), int)
                    ):
                        enum_updates[f.name] = f.type(v)
                yield dataclasses.replace(r, **enum_updates)
