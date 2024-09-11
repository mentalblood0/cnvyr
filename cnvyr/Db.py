import contextlib
import dataclasses
import datetime
import enum
import logging
import typing

import asyncstdlib
import psycopg
import psycopg.rows
import psycopg.sql


@dataclasses.dataclass(frozen=True, kw_only=True)
class Item:
    id: int | None = dataclasses.field(default=None, compare=False)
    created: datetime.datetime
    digest: bytes

    def __post_init__(self):
        for f in dataclasses.fields(self):
            if f.type | type(value := getattr(self, f.name)) != f.type:
                raise ValueError(
                    f"{self.__class__.__name__}.{f.name} expects value of type {f.type}, "
                    f"got {value} of type {type(value)}"
                )

    @classmethod
    async def load_from(cls, db: "Db", query: str):
        async for d in db._load(query, cls):
            yield cls(**d)


@dataclasses.dataclass(frozen=False, kw_only=True)
class Db:
    user: str
    password: str
    name: str
    host: str = "127.0.0.1"
    port: int = 5432

    async def init(self):
        self._connection = await self._new_connection
        self._enum_values_cache: set[str] = set()

    @property
    async def _new_connection(self):
        while True:
            try:
                return await psycopg.AsyncConnection.connect(
                    f"host={self.host} port={self.port} dbname={self.name} user={self.user} password={self.password}",
                    autocommit=True,
                )
            except Exception as e:
                logging.error(f"Exception ({e.__class__.__name__}, {e}) when connecting to db {self}")

    @asyncstdlib.cached_property
    async def _create_log_table(self):
        await self._create_enum
        async with self._connection.cursor() as acur:
            await acur.execute(
                "create table if not exists cnvyr_log (id bigserial primary key not null, "
                "datetime timestamp default(now() at time zone 'utc') not null, "
                "item_type cnvyr_enum not null, item_id bigint not null, operation cnvyr_enum not null, "
                "key cnvyr_enum not null, value text not null)"
            )
            await acur.execute("create index if not exists cnvyr_log_datetime on cnvyr_log(datetime)")
            await acur.execute("create index if not exists cnvyr_log_item_type on cnvyr_log(item_type)")
            await acur.execute("create index if not exists cnvyr_log_item_id on cnvyr_log(item_id)")
            await acur.execute("create index if not exists cnvyr_log_operation on cnvyr_log(operation)")
            await acur.execute("create index if not exists cnvyr_log_key on cnvyr_log(key)")
            await acur.execute("create index if not exists cnvyr_log_value on cnvyr_log(value)")

    @asyncstdlib.cached_property
    async def _create_errors_table(self):
        await self._create_enum
        async with self._connection.cursor() as acur:
            await acur.execute(
                "create table if not exists cnvyr_errors (id bigserial primary key not null, "
                "first timestamp default(now() at time zone 'utc') not null, "
                "last timestamp default(now() at time zone 'utc') not null, "
                "amount bigint default(1), operation cnvyr_enum not null, "
                "error_type cnvyr_enum not null, error_text text not null, unique (operation, error_type, error_text))"
            )
            await acur.execute("create index if not exists cnvyr_errors_first on cnvyr_errors(first)")
            await acur.execute("create index if not exists cnvyr_errors_last on cnvyr_errors(last)")
            await acur.execute("create index if not exists cnvyr_errors_amount on cnvyr_errors(amount)")
            await acur.execute("create index if not exists cnvyr_errors_operation on cnvyr_errors(operation)")
            await acur.execute("create index if not exists cnvyr_errors_error_type on cnvyr_errors(error_type)")
            await acur.execute("create index if not exists cnvyr_errors_error_text on cnvyr_errors(error_text)")

    @asyncstdlib.cached_property
    async def _create_enum(self):
        try:
            await self._connection.execute("create type cnvyr_enum as enum ()")
        except psycopg.errors.DuplicateObject:
            async for r in await self._connection.execute(
                "select e.enumlabel from pg_enum as e join pg_type as t on e.enumtypid=t.oid where t.typname=%s",
                ("cnvyr_enum",),
            ):
                self._enum_values_cache.add(r[0])

    def _enum_values(self, source: str | type[enum.Enum] | type[Item]):
        result: set[str] = set()
        if isinstance(source, str):
            result.add(source)
        elif isinstance(source, type) and issubclass(source, enum.Enum):
            result |= {e.name for e in source}
        elif isinstance(source, type) and issubclass(source, Item):
            result.add(source.__name__)
            for f in source.__dataclass_fields__.values():
                result.add(f.name)
                result |= self._enum_values(f.type)
        return result

    async def _add_enum_values(self, *source: str | type[enum.Enum] | type[Item]):
        names: set[str] = set()
        for s in source:
            names |= self._enum_values(s)

        await self._create_enum
        names -= self._enum_values_cache

        if names:
            async with self._connection.cursor() as acur:
                for n in names:
                    await acur.execute(f"alter type cnvyr_enum add value if not exists '{n}'")
            self._enum_values_cache |= {*names}

    def _table_name(self, item: Item):
        return type(item).__name__.lower()

    async def _create_table(self, c: Item, acur: psycopg.AsyncCursor):
        t_name = self._table_name(c)
        ct_query = f"create table if not exists {t_name}"
        fields = []
        ci_queries = []

        for f in dataclasses.fields(c):
            f_name = f.name.lower()
            if f_name == "id":
                fields.append(f"{f_name} bigserial primary key not null")
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
                    field.append("cnvyr_enum")
                else:
                    raise ValueError(f"can not convert type {f.type} to database type")

                if typing.Union[f.type, None] != f.type:
                    field.append("not null")

                fields.append(" ".join(field))

        ct_query += f"({', '.join(fields)})"

        for q in [ct_query] + ci_queries:
            await acur.execute(q)

    async def wipe(self):
        async with self._connection.cursor() as acur:
            await acur.execute("drop schema public cascade")
            await acur.execute("create schema public")
            await acur.execute("grant all on schema public to postgres")
            await acur.execute("grant all on schema public to public")

    async def _create(self, item: Item, acur: psycopg.AsyncCursor):
        await self._create_table(item, acur)
        query = f"insert into {self._table_name(item)}"

        fields = self._asdict(item)
        del fields["id"]
        query += "(" + ", ".join(fields) + ") values (" + ", ".join(f"%({k})s" for k in fields) + ")"
        query += " returning id"

        await acur.execute(query, fields)
        result = await acur.fetchone()

        if result is None:
            raise ValueError(f"result of insert is None")
        return result[0]

    def _asdict(self, item: Item):
        return {k: v.name if isinstance(v, enum.Enum) else v for k, v in dataclasses.asdict(item).items()}

    def _diff(self, old: Item | None, new: Item):
        d_old = self._asdict(old) if old is not None else {}
        d_new = self._asdict(new)
        if old is not None:
            del d_old["id"]
        del d_new["id"]
        return dict(set(d_new.items()) - set(d_old.items()))

    async def _update(self, old: Item, new: Item, acur: psycopg.AsyncCursor):
        t_name = self._table_name(old)
        if t_name != (t_name_new := self._table_name(new)):
            raise ValueError(f"old item table name {t_name} != {t_name_new}")
        if old.id != new.id:
            raise ValueError(f"old item id {old.id} != {new.id}")

        query = f"update {t_name} set "

        diff = self._diff(old, new)
        if not diff:
            return
        for k, v in diff.items():
            if k in Item.__dataclass_fields__:
                raise ValueError(f"attempt to change constant field: ({k}, {v})")
        query += ", ".join(f"{k}=%({k})s" for k in diff)

        rdiff = self._diff(new, old)
        query += " where " + " and ".join(f"{k}=%(_{k})s" for k in rdiff.keys())

        await acur.execute(query, diff | {f"_{k}": v for k, v in rdiff.items()})
        result = acur.pgresult
        status = acur.statusmessage
        if not (result and status):
            raise ValueError(f"update resulted in {result}, status message is {status}")

    async def _log(self, operation: enum.Enum, old: Item | None, new: Item, acur: psycopg.AsyncCursor):
        await acur.executemany(
            "insert into cnvyr_log(item_type, item_id, operation, key, value) values(%s, %s, %s, %s, %s)",
            [
                (type(new).__name__, new.id, operation.name, k, str(v))
                for k, v in self._diff(old, new).items()
                if not ((old is None) and (v is None))
            ],
        )

    async def transaction(self, operation: enum.Enum, *actions: Item | tuple[Item, Item]):
        await self._add_enum_values(type(operation), *[type(a) if isinstance(a, Item) else type(a[1]) for a in actions])
        await self._create_log_table
        async with self._connection.cursor() as acur:
            async with self._connection.transaction():
                for a in actions:
                    if isinstance(a, Item):
                        received_id = await self._create(a, acur)
                        await self._log(operation, None, dataclasses.replace(a, id=received_id), acur)
                    elif isinstance(a, tuple) and len(a) == 2 and isinstance(a[0], Item) and isinstance(a[1], Item):
                        await self._log(operation, *a, acur)
                        await self._update(*a, acur)
                    else:
                        raise ValueError(f"expect Item or two-Item tuple, got {a}")

    @contextlib.asynccontextmanager
    async def error_logging(self, operation: enum.Enum):
        try:
            await self._create_errors_table
            await self._add_enum_values(type(operation))
            yield
            await self._connection.execute("delete from cnvyr_errors where operation=%s", (operation.name,))
        except Exception as e:
            error_type = type(e).__name__
            while True:
                try:
                    await self._add_enum_values(error_type)
                    await self._connection.execute(
                        "insert into cnvyr_errors(operation, error_type, error_text) values (%s, %s, %s) "
                        "on conflict (operation, error_type, error_text) do update "
                        "set last=now() at time zone 'utc', amount=cnvyr_errors.amount+1",
                        (operation.name, error_type, str(e)),
                    )
                    break
                except Exception as db_e:
                    logging.error(
                        f"Exception ({db_e.__class__.__name__}, {db_e}) when trying to log "
                        f"exception ({error_type}, {e}) to db"
                    )
                    self._connection = await self._new_connection

    async def _load(self, query: str, t: type[Item]):
        async with self._connection.cursor(row_factory=psycopg.rows.dict_row) as acur:
            await acur.execute(query)
            async for d in acur:
                for f in dataclasses.fields(t):
                    if isinstance(f.type, type) and issubclass(f.type, enum.Enum):
                        if not isinstance((v := d[f.name]), str):
                            raise ValueError(
                                f"expected string value for field named {f.name}, got {v} of type {type(v)}"
                            )
                        d[f.name] = getattr(f.type, v)
                yield d
