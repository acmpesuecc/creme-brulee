import json
import base64
import random
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Callable, Any, Iterator, Self, TypeVar


T = TypeVar("T")

NUM_ROWS = 100  # Number of rows to generate
DDOS = 10
START_TIME = datetime.now() - timedelta(days=1)
END_TIME = datetime.now()

ACCESS_SCHEMA = ["time", "ip", "end_point"]
PEOPLE_SCHEMA = ["time", "person_name", "location_id"]
SUBNET_SCHEMA = ["location_id", "location_name", "subnet_start", "subnet_end"]

ENDPOINTS = [
    "/users",
    "/leaderboard",
    "/food",
    "/dinner",
    "/register",
    "/pay",
]
LOCATIONS = [
    "bathroom",
    "seminar-hall-1",
    "seminar-hall-2",
    "canteen-4-floor",
    "canteen-5-floor",
    "canteen-pixel-block",
    "mrd-auditorium",
]
PERSON_NAMES = [
    "potukuchi",
    "naga",
    "sriprad",
    "reema",
    "adhesh",
    "gamblesh",
    "aditya",
    "sudhir",
    "bevdesh",
    "kiran",
    "achyuth",
    "suraj",
    "abhinav",
    "kaddapa",
    "rowjee",
    "🗿",
]


# Function to generate a mock IP address in the format 192.168.x.y
def generate_ip() -> str:
    x = random.randint(0, len(LOCATIONS) - 1)
    y = random.randint(0, 255)
    return f"{192}.{168}.{x}.{y}"


def generate_time(start_time: datetime, end_time: datetime) -> datetime:
    delta = end_time - start_time
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_time + timedelta(seconds=random_seconds)


def interleave(*iters: Iterator[T]) -> Iterator[T]:
    liters = list(iters)
    while liters:
        chosen = min(random.binomialvariate(1, 0.7), len(liters) - 1)
        it = liters[chosen]
        try:
            yield next(it)
        except StopIteration:
            liters.remove(it)


def repeat(genrec: Callable[[], T], times: int) -> Iterator[T]:
    for _ in range(times):
        yield genrec()


class FileWriter(ABC):
    @abstractmethod
    def __init__(self, filename: str) -> None:
        pass

    @abstractmethod
    def write_access(self, generator: Iterator[list[Any]]) -> None:
        pass

    @abstractmethod
    def write_people(self, generator: Iterator[list[Any]]) -> None:
        pass

    @abstractmethod
    def write_subnet(self, generator: Iterator[list[Any]]) -> None:
        pass

    @abstractmethod
    def init_tables(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class JSONFileWriter(FileWriter):
    def __init__(self, filename: str):
        self.is_first_key = True
        self.json_file = open(f"{filename}.json", "w")
        self.json_file.write("{")

    def init_tables(self) -> None:
        pass

    def _write_key(self, key: str) -> None:
        if not self.is_first_key:
            self.json_file.write(",")
        self.json_file.write(f'"{key}": [')
        self.is_first_key = False

    def _write_records(self, schema: list[str], generator: Iterator[list[Any]]) -> None:
        is_first_record = True
        for record in generator:
            if not is_first_record:
                self.json_file.write(",")
            json.dump(dict(zip(schema, map(str, record))), self.json_file)
            is_first_record = False
        self.json_file.write("]")

    def write_access(self, generator: Iterator[list[Any]]) -> None:
        self._write_key("access")
        self._write_records(ACCESS_SCHEMA, generator)

    def write_people(self, generator: Iterator[list[Any]]) -> None:
        self._write_key("people")
        self._write_records(PEOPLE_SCHEMA, generator)

    def write_subnet(self, generator: Iterator[list[Any]]) -> None:
        self._write_key("subnet")
        self._write_records(SUBNET_SCHEMA, generator)

    def close(self):
        self.json_file.write("}")
        self.json_file.close()


class SqliteWriter(FileWriter):
    def __init__(self, filename: str):
        self.db = sqlite3.connect(f"{filename}.db")
        self.cursor = self.db.cursor()

    def close(self) -> None:
        self.db.commit()
        self.db.close()

    def _write_to_db(
        self, table_name: str, schema: list[str], generator: Iterator[list[Any]]
    ) -> None:
        columns = ", ".join(schema)
        placeholders = ", ".join(["?"] * len(schema))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        for record in generator:
            self.cursor.execute(query, list(map(str, record)))

        self.db.commit()

    def write_access(self, generator: Iterator[list[Any]]) -> None:
        self._write_to_db("access", ACCESS_SCHEMA, generator)

    def write_people(self, generator: Iterator[list[Any]]) -> None:
        self._write_to_db("people", PEOPLE_SCHEMA, generator)

    def write_subnet(self, generator: Iterator[list[Any]]) -> None:
        self._write_to_db("subnet", SUBNET_SCHEMA, generator)

    def init_tables(self) -> None:
        def add_type(field: str) -> str:
            return f"{field} TEXT"

        access_schema = ", ".join(map(add_type, ACCESS_SCHEMA))
        people_schema = ", ".join(map(add_type, PEOPLE_SCHEMA))
        subnet_schema = ", ".join(map(add_type, SUBNET_SCHEMA))

        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS access({access_schema})")
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS people({people_schema})")
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS subnet({subnet_schema})")
        self.db.commit()


class MockDB:
    def __init__(self, dbid: int) -> None:
        self.dbid = dbid
        self.target_time = generate_time(START_TIME, END_TIME)
        self.target_ip = generate_ip()
        self.target_location = random.randint(0, len(LOCATIONS) - 1)
        self.target_subnet = random.randint(0, 255)
        self.target_person = str(
            base64.b64encode(random.choice(PERSON_NAMES).encode())
        )[2:-1]

    def __enter__(self) -> Self:
        self.writer = self.writer_class(f"challenge_{self.dbid}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.writer.close()

    def log_answer(self) -> None:
        print(
            "{"
            f'"database": {self.dbid}, '
            f'"time": "{str(self.target_time)}", '
            f'"name": "{base64.decodebytes(self.target_person.encode()).decode()}", '
            f'"ip": "{self.target_ip}", '
            f'"location": "{LOCATIONS[self.target_location]}"',
            "}",
        )

    def with_writer(self, writer_class: type[FileWriter]) -> Self:
        self.writer_class = writer_class

        return self

    def init_tables(self) -> Self:
        self.writer.init_tables()
        return self

    def write_tables(self) -> Self:
        self.writer.write_access(
            interleave(repeat(self.access_writer, NUM_ROWS), self.access_target())
        )
        self.writer.write_people(
            interleave(repeat(self.people_writer, NUM_ROWS), self.people_target())
        )
        self.writer.write_subnet(
            interleave(repeat(self.subnet_writer, 0), self.subnet_target())
        )
        return self

    def access_writer(self) -> list[Any]:
        time_stamp = generate_time(START_TIME, END_TIME)
        ip = generate_ip()
        endpoint = random.choice(ENDPOINTS)
        return [time_stamp, ip, endpoint]

    def access_target(self) -> Iterator[list[Any]]:
        for i in range(DDOS):
            yield [
                self.target_time + timedelta(seconds=random.randint(1, 15)),
                self.target_ip,
                random.choice(ENDPOINTS),
            ]

    def people_writer(self) -> list[Any]:
        time_stamp = generate_time(START_TIME, END_TIME)
        person = str(base64.b64encode(random.choice(PERSON_NAMES).encode()))[2:-1]
        loc_id = random.choice(range(len(LOCATIONS)))
        if (
            self.target_time < time_stamp < self.target_time + timedelta(seconds=15)
            and loc_id == self.target_location
        ):
            time_stamp += timedelta(seconds=30)
        return [time_stamp, person, loc_id]

    def people_target(self) -> Iterator[list[Any]]:
        yield [self.target_time, self.target_person, self.target_location]

    def subnet_writer(self) -> list[str]:
        raise Exception("There is no subnet writer")

    def subnet_target(self) -> Iterator[list[Any]]:
        start = random.randint(max(self.target_subnet - 20, 0), 255)
        end = random.randint(start, 255)
        target_subnet_entry = [
            self.target_location,
            LOCATIONS[self.target_location],
            f"192.168.{self.target_location}.{start}",
            f"192.168.{self.target_location}.{end}",
        ]
        for x in range(len(LOCATIONS)):
            if x == self.target_location:
                yield target_subnet_entry
                continue
            start = random.randint(0, 255)
            end = random.randint(start, 255)
            yield [
                x,
                LOCATIONS[x],
                f"192.168.{x}.{start}",
                f"192.168.{x}.{end}",
            ]


def gen_db(dbidx: int) -> None:
    with MockDB(dbidx).with_writer(JSONFileWriter) as mdb:
        mdb.init_tables().write_tables().log_answer()


if __name__ == "__main__":
    gen_db(0)
