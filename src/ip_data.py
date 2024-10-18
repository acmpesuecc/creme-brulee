import csv
import json
import base64
import random
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Callable, Any, Iterator, Self, TypeVar, List


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


def interleave(iters: list[Iterator[T]]) -> Iterator[T]:
    while iters:
        chosen = min(random.binomialvariate(1, 0.7), len(iters) - 1)
        it = iters[chosen]
        try:
            yield next(it)
        except StopIteration:
            iters.remove(it)


def repeat(genrec: Callable[[], T], times: int) -> Iterator[T]:
    for _ in range(times):
        yield genrec()


class FileWriter(ABC):
    def __init__(self, base_filename: str):
        self.base_filename = base_filename

    @abstractmethod
    def write_access(self, generator: Iterator[List[Any]]) -> None:
        pass

    @abstractmethod
    def write_people(self, generator: Iterator[List[Any]]) -> None:
        pass

    @abstractmethod
    def write_subnet(self, generator: Iterator[List[Any]]) -> None:
        pass

    @abstractmethod
    def init_tables(self) -> None:
        pass

    @abstractmethod
    def __enter__(self):
        return self

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class JSONFileWriter(FileWriter):
    def _write_to_json(self, filename: str, schema: List[str], generator: Iterator[List[Any]]) -> None:
        result = []
        for record in generator:
            result.append(dict(zip(schema, map(str, record))))
        with open(f"{self.base_filename}_{filename}.json", mode="w") as file:
            json.dump(result, file, indent=2)

    def write_access(self, generator: Iterator[List[Any]]) -> None:
        self._write_to_json("access", ACCESS_SCHEMA, generator)

    def write_people(self, generator: Iterator[List[Any]]) -> None:
        self._write_to_json("people", PEOPLE_SCHEMA, generator)

    def write_subnet(self, generator: Iterator[List[Any]]) -> None:
        self._write_to_json("subnet", SUBNET_SCHEMA, generator)

    def init_tables(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class SqliteWriter(FileWriter):
    def __init__(self, base_filename: str):
        super().__init__(base_filename)
        self.db = sqlite3.connect(f"{self.base_filename}.db")
        self.cursor = self.db.cursor()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.db.commit()
        self.db.close()

    def _write_to_db(self, table_name: str, schema: List[str], generator: Iterator[List[Any]]) -> None:
        columns = ", ".join(schema)
        placeholders = ", ".join(["?"] * len(schema))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        for record in generator:
            self.cursor.execute(query, record)

        self.db.commit()

    def write_access(self, generator: Iterator[List[Any]]) -> None:
        self._write_to_db("access", ACCESS_SCHEMA, generator)

    def write_people(self, generator: Iterator[List[Any]]) -> None:
        self._write_to_db("people", PEOPLE_SCHEMA, generator)

    def write_subnet(self, generator: Iterator[List[Any]]) -> None:
        self._write_to_db("subnet", SUBNET_SCHEMA, generator)

    def init_tables(self) -> None:
        def add_type(field: str) -> str:
            return f"{field} TEXT"

        access_schema = ", ".join(map(add_type, ACCESS_SCHEMA))
        people_schema = ", ".join(map(add_type, PEOPLE_SCHEMA))
        subnet_schema = ", ".join(map(add_type, SUBNET_SCHEMA))

        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS access({access_schema})")
        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS people({people_schema})")
        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS subnet({subnet_schema})")
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
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

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

    def write_to_file(
        self,
        schema: list[str],
        tablename: str,
        targets: Iterator[list[Any]],
        normal: Iterator[list[Any]],
    ) -> None:
        with open(tablename, mode="w") as file:
            writer = csv.writer(file)
            writer.writerow(schema)
            for record in interleave([targets, normal]):
                writer.writerow(record)

    def with_writer(self, writer_class: type[FileWriter]) -> Self:
        self.writer_class = writer_class

        return self

    def write_tables(self) -> Self:
        with self.writer_class(f"challenge_{self.dbid}") as writer:
            writer.init_tables()
            writer.write_access(repeat(self.access_writer, NUM_ROWS))
            writer.write_people(repeat(self.people_writer, NUM_ROWS))
            writer.write_subnet(self.subnet_target())
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
        person = str(base64.b64encode(
            random.choice(PERSON_NAMES).encode()))[2:-1]
        loc_id = random.choice(range(len(LOCATIONS)))
        if (
            self.target_time < time_stamp < self.target_time +
                timedelta(seconds=15)
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
    with MockDB(dbidx) as mdb:
        mdb.with_writer(JSONFileWriter).write_tables().log_answer()


if __name__ == "__main__":
    gen_db(0)
