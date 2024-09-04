import dataclasses
import datetime
import typing

import psycopg

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

    def create_table(self, c: Item):
        t_name = type(c).__name__.lower()
        ct_query = f"create table if not exists {t_name}"
        fields = []
        ci_queries = []

        for f in dataclasses.fields(c):
            f_name = f.name.lower()
            if f_name == "_id":
                fields.append(f"{f_name} bigserial not null")
            elif f_name == "_digest":
                fields.append(f"{f_name} bit(512) not null")
            elif f_name == "_created":
                fields.append(f"{f_name} timestamp not null")
            else:

                ci_queries.append(f"create index if not exists {t_name}_{f_name} on {t_name}({f_name})")
                field = [f_name]

                if f.type | bool == f.type:
                    field.append("boolean")
                elif f.type | str == f.type:
                    field.append("text")
                elif f.type | int == f.type:
                    field.append("bigint")
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
                print(q)
                cursor.execute(q)
