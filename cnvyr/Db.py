import dataclasses
import datetime
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

    def create(self, item: Item):
        self.create_table(item)
        query = f"insert into {self.table_name(item)}"
        fields = dataclasses.asdict(item)
        del fields["id"]
        query += "(" + ", ".join(fields) + ") values (" + ", ".join(f"%({k})s" for k in fields) + ")"
        self.connection.execute(query, fields)

    def update(self, old: Item, new: Item):
        t_name = self.table_name(old)
        if t_name != (t_name_new := self.table_name(new)):
            raise ValueError(f"old item table name {t_name} != {t_name_new}")
        if old.id != new.id:
            raise ValueError(f"old item id {old.id} != {new.id}")

        query = f"update {t_name} set "
        d_old = dataclasses.asdict(old)
        d_new = dataclasses.asdict(new)

        diff = dict(list(set(d_new.items()) - set(d_old.items())))
        if not diff:
            return
        for k, v in diff.items():
            if k in Item.__dataclass_fields__:
                raise ValueError(f"attempt to change constant field: ({k}, {v})")
        query += ", ".join(f"{k}=%({k})s" for k in diff)

        del d_old["id"]
        where_old = {f"_old_{k}": v for k, v in d_old.items()}
        query += " where " + " and ".join(f"{k}=%(_old_{k})s" for k in list(d_old.keys()))

        self.connection.execute(query, diff | where_old)

    def load(self, query: str, t: type[Item]):
        with self.connection.cursor(row_factory=psycopg.rows.class_row(t)) as cursor:
            for r in cursor.execute(query):
                yield r
