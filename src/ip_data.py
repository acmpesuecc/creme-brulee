import csv
import random
from datetime import datetime, timedelta
from typing import Callable, Any, Iterator, TypeVar
import base64
import sqlite3

T = TypeVar("T")

def generate_ip(locations_count: int) -> str:
    x = random.randint(0, locations_count - 1)
    y = random.randint(0, 255)
    return f"{192}.{168}.{x}.{y}"

def generate_time(start_time: datetime, end_time: datetime) -> datetime:
    delta = end_time - start_time
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_time + timedelta(seconds=random_seconds)

def interleave(iters: list[Iterator[T]]) -> Iterator[T]:
    while iters:
        chosen = min(random.randint(0, len(iters) - 1), len(iters) - 1)
        it = iters[chosen]
        try:
            yield next(it)
        except StopIteration:
            iters.remove(it)

def repeat(genrec: Callable[[], T], times: int) -> Iterator[T]:
    for _ in range(times):
        yield genrec()

class MockDB:
    def __init__(self, dbid: int, num_rows: int = 100, ddos: int = 10, 
                 start_time: datetime = None, end_time: datetime = None,
                 access_schema=None, people_schema=None, subnet_schema=None,
                 endpoints=None, locations=None, person_names=None) -> None:
        self.dbid = dbid
        self.num_rows = num_rows
        self.ddos = ddos
        self.start_time = start_time or (datetime.now() - timedelta(days=1))
        self.end_time = end_time or datetime.now()
        
        self.access_schema = access_schema or ["time", "ip", "end_point"]
        self.people_schema = people_schema or ["time", "person_name", "location_id"]
        self.subnet_schema = subnet_schema or ["location_id", "location_name", "subnet_start", "subnet_end"]

        self.endpoints = endpoints or [
            "/users", "/leaderboard", "/food", "/dinner", "/register", "/pay",
        ]
        self.locations = locations or [
            "bathroom", "seminar-hall-1", "seminar-hall-2", "canteen-4-floor", 
            "canteen-5-floor", "canteen-pixel-block", "mrd-auditorium",
        ]
        self.person_names = person_names or [
            "potukuchi", "naga", "sriprad", "reema", "adhesh", "gamblesh", 
            "aditya", "sudhir", "bevdesh", "kiran", "achyuth", "suraj", 
            "abhinav", "kaddapa", "rowjee", "🗿",
        ]

        self.target_time = generate_time(self.start_time, self.end_time)
        self.target_ip = generate_ip(len(self.locations))
        self.target_location = random.randint(0, len(self.locations) - 1)
        self.target_subnet = random.randint(0, 255)
        self.target_person = str(
            base64.b64encode(random.choice(self.person_names).encode())
        )[2:-1]

    def __enter__(self) -> 'MockDB':
        self.db = sqlite3.connect(f"challenge_{self.dbid}.db")
        self.cursor = self.db.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.db.commit()
        self.db.close()

    def log_answer(self) -> None:
        print(
            "{"
            f'"database": {self.dbid}, '
            f'"time": "{str(self.target_time)}", '
            f'"name": "{base64.decodebytes(self.target_person.encode()).decode()}", '
            f'"ip": "{self.target_ip}", '
            f'"location": "{self.locations[self.target_location]}"',
            "}",
        )

    def init_tables(self) -> 'MockDB':
        def add_type(fi: str) -> str:
            return fi + " TEXT"

        access_q = ",".join(map(add_type, self.access_schema))
        people_q = ",".join(map(add_type, self.people_schema))
        subnet_q = ",".join(map(add_type, self.subnet_schema))

        self.cursor.execute(f"CREATE TABLE access({access_q})")
        self.cursor.execute(f"CREATE TABLE people({people_q})")
        self.cursor.execute(f"CREATE TABLE subnet({subnet_q})")
        self.db.commit()
        return self

    def write_to_db(
        self,
        schema: list[str],
        tablename: str,
        targets: Iterator[list[Any]],
        normal: Iterator[list[Any]],
    ) -> None:
        params = ", ".join(["?"] * len(schema))
        for record in interleave([targets, normal]):
            self.cursor.execute(
                f"INSERT INTO {tablename} VALUES ({params})",
                list(map(str, record)),
            )
        self.db.commit()

    def write_tables(self) -> 'MockDB':
        self.write_to_db(
            self.access_schema,
            "access",
            self.access_target(),
            repeat(self.access_writer, self.num_rows),
        )
        self.write_to_db(
            self.people_schema,
            "people",
            self.people_target(),
            repeat(self.people_writer, self.num_rows),
        )
        self.write_to_db(
            self.subnet_schema,
            "subnet",
            self.subnet_target(),
            repeat(self.subnet_writer, 0),
        )
        return self

    def access_writer(self) -> list[Any]:
        time_stamp = generate_time(self.start_time, self.end_time)
        ip = generate_ip(len(self.locations))
        endpoint = random.choice(self.endpoints)
        return [time_stamp, ip, endpoint]

    def access_target(self) -> Iterator[list[Any]]:
        for _ in range(self.ddos):
            yield [
                self.target_time + timedelta(seconds=random.randint(1, 15)),
                self.target_ip,
                random.choice(self.endpoints),
            ]

    def people_writer(self) -> list[Any]:
        time_stamp = generate_time(self.start_time, self.end_time)
        person = str(base64.b64encode(random.choice(self.person_names).encode()))[2:-1]
        loc_id = random.choice(range(len(self.locations)))
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
            self.locations[self.target_location],
            f"192.168.{self.target_location}.{start}",
            f"192.168.{self.target_location}.{end}",
        ]
        for x in range(len(self.locations)):
            if x == self.target_location:
                yield target_subnet_entry
                continue
            start = random.randint(0, 255)
            end = random.randint(start, 255)
            yield [
                x,
                self.locations[x],
                f"192.168.{x}.{start}",
                f"192.168.{x}.{end}",
            ]

def gen_db(dbidx: int) -> None:
    with MockDB(dbidx) as mdb:
        mdb.init_tables().write_tables().log_answer()

if __name__ == "__main__":
    gen_db(0)
