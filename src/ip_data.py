import csv
import random
from datetime import datetime, timedelta
from typing import Callable, Any, Iterator, Self, TypeVar, List
import base64
import sqlite3

T = TypeVar("T")

class MockDB:
    ACCESS_SCHEMA = ["time", "ip", "end_point"]
    PEOPLE_SCHEMA = ["time", "person_name", "location_id"]
    SUBNET_SCHEMA = ["location_id", "location_name", "subnet_start", "subnet_end"]

    ENDPOINTS = [
        "/users", "/leaderboard", "/food", "/dinner", "/register", "/pay",
    ]
    LOCATIONS = [
        "bathroom", "seminar-hall-1", "seminar-hall-2", "canteen-4-floor", 
        "canteen-5-floor", "canteen-pixel-block", "mrd-auditorium",
    ]
    PERSON_NAMES = [
        "potukuchi", "naga", "sriprad", "reema", "adhesh", "gamblesh", 
        "aditya", "sudhir", "bevdesh", "kiran", "achyuth", "suraj", 
        "abhinav", "kaddapa", "rowjee", "🗿",
    ]

    def __init__(self, dbid: int, num_rows: int, ddos: int, 
                 start_time: datetime, end_time: datetime) -> None:
        self.dbid = dbid
        self.num_rows = num_rows
        self.ddos = ddos
        self.start_time = start_time
        self.end_time = end_time
        
        self.target_time = self.generate_time(self.start_time, self.end_time)
        self.target_ip = self.generate_ip()
        self.target_location = random.randint(0, len(self.LOCATIONS) - 1)
        self.target_subnet = random.randint(0, 255)
        self.target_person = str(
            base64.b64encode(random.choice(self.PERSON_NAMES).encode())
        )[2:-1]

    def __enter__(self) -> Self:
        self.db = sqlite3.connect(f"challenge_{self.dbid}.db")
        self.cursor = self.db.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.db.commit()
        self.db.close()

    def generate_ip(self) -> str:
        x = random.randint(0, len(self.LOCATIONS) - 1)
        y = random.randint(0, 255)
        return f"{192}.{168}.{x}.{y}"

    def generate_time(self, start_time: datetime, end_time: datetime) -> datetime:
        delta = end_time - start_time
        random_seconds = random.randint(0, int(delta.total_seconds()))
        return start_time + timedelta(seconds=random_seconds)

    def interleave(self, iters: List[Iterator[T]]) -> Iterator[T]:
        while iters:
            chosen = min(random.randint(0, len(iters) - 1), len(iters) - 1)
            it = iters[chosen]
            try:
                yield next(it)
            except StopIteration:
                iters.remove(it)

    def repeat(self, genrec: Callable[[], T], times: int) -> Iterator[T]:
        for _ in range(times):
            yield genrec()

    def log_answer(self) -> None:
        print(
            "{"
            f'"database": {self.dbid}, '
            f'"time": "{str(self.target_time)}", '
            f'"name": "{base64.decodebytes(self.target_person.encode()).decode()}", '
            f'"ip": "{self.target_ip}", '
            f'"location": "{self.LOCATIONS[self.target_location]}"',
            "}",
        )

    def init_tables(self) -> Self:
        access_q = ",".join(f"{field} TEXT" for field in self.ACCESS_SCHEMA)
        people_q = ",".join(f"{field} TEXT" for field in self.PEOPLE_SCHEMA)
        subnet_q = ",".join(f"{field} TEXT" for field in self.SUBNET_SCHEMA)

        self.cursor.execute(f"CREATE TABLE access({access_q})")
        self.cursor.execute(f"CREATE TABLE people({people_q})")
        self.cursor.execute(f"CREATE TABLE subnet({subnet_q})")
        self.db.commit()
        return self

    def write_to_db(
        self,
        schema: List[str],
        tablename: str,
        targets: Iterator[List[Any]],
        normal: Iterator[List[Any]],
    ) -> None:
        params = ", ".join(["?"] * len(schema))
        for record in self.interleave([targets, normal]):
            self.cursor.execute(
                f"INSERT INTO {tablename} VALUES ({params})",
                list(map(str, record)),
            )
        self.db.commit()

    def write_tables(self) -> Self:
        self.write_to_db(
            self.ACCESS_SCHEMA,
            "access",
            self.access_target(),
            self.repeat(self.access_writer, self.num_rows),
        )
        self.write_to_db(
            self.PEOPLE_SCHEMA,
            "people",
            self.people_target(),
            self.repeat(self.people_writer, self.num_rows),
        )
        self.write_to_db(
            self.SUBNET_SCHEMA,
            "subnet",
            self.subnet_target(),
            self.repeat(self.subnet_writer, 0),
        )
        return self

    def access_writer(self) -> List[Any]:
        time_stamp = self.generate_time(self.start_time, self.end_time)
        ip = self.generate_ip()
        endpoint = random.choice(self.ENDPOINTS)
        return [time_stamp, ip, endpoint]

    def access_target(self) -> Iterator[List[Any]]:
        for _ in range(self.ddos):
            yield [
                self.target_time + timedelta(seconds=random.randint(1, 15)),
                self.target_ip,
                random.choice(self.ENDPOINTS),
            ]

    def people_writer(self) -> List[Any]:
        time_stamp = self.generate_time(self.start_time, self.end_time)
        person = str(base64.b64encode(random.choice(self.PERSON_NAMES).encode()))[2:-1]
        loc_id = random.choice(range(len(self.LOCATIONS)))
        if (
            self.target_time < time_stamp < self.target_time + timedelta(seconds=15)
            and loc_id == self.target_location
        ):
            time_stamp += timedelta(seconds=30)
        return [time_stamp, person, loc_id]

    def people_target(self) -> Iterator[List[Any]]:
        yield [self.target_time, self.target_person, self.target_location]

    def subnet_writer(self) -> List[str]:
        raise Exception("There is no subnet writer")

    def subnet_target(self) -> Iterator[List[Any]]:
        start = random.randint(max(self.target_subnet - 20, 0), 255)
        end = random.randint(start, 255)
        target_subnet_entry = [
            self.target_location,
            self.LOCATIONS[self.target_location],
            f"192.168.{self.target_location}.{start}",
            f"192.168.{self.target_location}.{end}",
        ]
        for x in range(len(self.LOCATIONS)):
            if x == self.target_location:
                yield target_subnet_entry
                continue
            start = random.randint(0, 255)
            end = random.randint(start, 255)
            yield [
                x,
                self.LOCATIONS[x],
                f"192.168.{x}.{start}",
                f"192.168.{x}.{end}",
            ]


class MockDBBuilder:
    def __init__(self):
        self.dbid = 0
        self.num_rows = 100
        self.ddos = 10
        self.start_time = datetime.now() - timedelta(days=1)
        self.end_time = datetime.now()

    def with_dbid(self, dbid: int) -> Self:
        self.dbid = dbid
        return self

    def with_rows(self, num_rows: int) -> Self:
        self.num_rows = num_rows
        return self

    def with_ddos(self, ddos: int) -> Self:
        self.ddos = ddos
        return self

    def build(self) -> MockDB:
        return MockDB(
            self.dbid,
            self.num_rows,
            self.ddos,
            self.start_time,
            self.end_time
        )


def gen_db(dbidx: int) -> None:
    with MockDBBuilder().with_dbid(dbidx).with_rows(10000).with_ddos(4000).build() as mdb:
        mdb.init_tables().write_tables().log_answer()


if __name__ == "__main__":
    gen_db(0)
