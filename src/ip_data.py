import json
import base64
import random
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Callable, Any, Iterator, Self, TypeVar
from dataclasses import dataclass

T = TypeVar("T")

@dataclass
class MockDBConfig:
    num_rows: int = 100
    ddos_attempts: int = 10
    start_time: datetime = datetime.now() - timedelta(days=1)
    end_time: datetime = datetime.now()
    endpoints: list[str] = None
    locations: list[str] = None
    person_names: list[str] = None

    def __post_init__(self):
        # Default values if not provided
        self.endpoints = self.endpoints or [
            "/users", "/leaderboard", "/food", "/dinner", "/register", "/pay"
        ]
        self.locations = self.locations or [
            "bathroom", "seminar-hall-1", "seminar-hall-2", "canteen-4-floor",
            "canteen-5-floor", "canteen-pixel-block", "mrd-auditorium"
        ]
        self.person_names = self.person_names or [
            "potukuchi", "naga", "sriprad", "reema", "adhesh", "gamblesh",
            "aditya", "sudhir", "bevdesh", "kiran", "achyuth", "suraj",
            "abhinav", "kaddapa", "rowjee", "🗿"
        ]

class FileWriter(ABC):
    @abstractmethod
    def __init__(self, filename: str, schemas: list[list[str]]) -> None:
        pass

    @abstractmethod
    def write_table(self, table_name: str, schema_idx: int, generator: Iterator[list[Any]]) -> None:
        pass

    @abstractmethod
    def init_tables(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

class JSONFileWriter(FileWriter):
    def __init__(self, filename: str, schemas: list[list[str]]):
        self.schemas = schemas
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

    def write_table(self, table_name: str, schema_idx: int, generator: Iterator[list[Any]]) -> None:
        self._write_key(table_name)
        is_first_record = True
        for record in generator:
            if not is_first_record:
                self.json_file.write(",")
            json.dump(dict(zip(self.schemas[schema_idx], map(str, record))), self.json_file)
            is_first_record = False
        self.json_file.write("]")

    def close(self):
        self.json_file.write("}")
        self.json_file.close()

class SqliteWriter(FileWriter):
    def __init__(self, filename: str, schemas: list[list[str]]):
        self.schemas = schemas
        self.db = sqlite3.connect(f"{filename}.db")
        self.cursor = self.db.cursor()

    def init_tables(self) -> None:
        for idx, schema in enumerate(self.schemas):
            table_name = f"table_{idx}"
            schema_str = ", ".join(f"{field} TEXT" for field in schema)
            self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name}({schema_str})")
        self.db.commit()

    def write_table(self, table_name: str, schema_idx: int, generator: Iterator[list[Any]]) -> None:
        schema = self.schemas[schema_idx]
        columns = ", ".join(schema)
        placeholders = ", ".join(["?"] * len(schema))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        for record in generator:
            self.cursor.execute(query, list(map(str, record)))
        self.db.commit()

    def close(self) -> None:
        self.db.commit()
        self.db.close()

class MockDB:
    def __init__(self, dbid: int, config: MockDBConfig, schemas: list[list[str]]) -> None:
        self.dbid = dbid
        self.config = config
        self.schemas = schemas
        self.target_time = self._generate_time()
        self.target_ip = self._generate_ip()
        self.target_location = random.randint(0, len(self.config.locations) - 1)
        self.target_subnet = random.randint(0, 255)
        self.target_person = str(
            base64.b64encode(random.choice(self.config.person_names).encode())
        )[2:-1]

    def _generate_ip(self) -> str:
        x = random.randint(0, len(self.config.locations) - 1)
        y = random.randint(0, 255)
        return f"{192}.{168}.{x}.{y}"

    def _generate_time(self) -> datetime:
        delta = self.config.end_time - self.config.start_time
        random_seconds = random.randint(0, int(delta.total_seconds()))
        return self.config.start_time + timedelta(seconds=random_seconds)

    def __enter__(self) -> Self:
        self.writer = self.writer_class(f"challenge_{self.dbid}", self.schemas)
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
            f'"location": "{self.config.locations[self.target_location]}"',
            "}",
        )

    def with_writer(self, writer_class: type[FileWriter]) -> Self:
        self.writer_class = writer_class
        return self

    def init_tables(self) -> Self:
        self.writer.init_tables()
        return self

    def write_tables(self) -> Self:
        # Write access logs (table 0)
        self.writer.write_table(
            "access", 0,
            interleave(repeat(self.access_writer, self.config.num_rows), 
                      self.access_target())
        )
        
        # Write people logs (table 1)
        self.writer.write_table(
            "people", 1,
            interleave(repeat(self.people_writer, self.config.num_rows), 
                      self.people_target())
        )
        
        # Write subnet logs (table 2)
        self.writer.write_table(
            "subnet", 2,
            interleave(repeat(self.subnet_writer, 0), self.subnet_target())
        )
        return self

    def access_writer(self) -> list[Any]:
        time_stamp = self._generate_time()
        ip = self._generate_ip()
        endpoint = random.choice(self.config.endpoints)
        return [time_stamp, ip, endpoint]

    def access_target(self) -> Iterator[list[Any]]:
        for _ in range(self.config.ddos_attempts):
            yield [
                self.target_time + timedelta(seconds=random.randint(1, 15)),
                self.target_ip,
                random.choice(self.config.endpoints),
            ]

    def people_writer(self) -> list[Any]:
        time_stamp = self._generate_time()
        person = str(base64.b64encode(random.choice(self.config.person_names).encode()))[2:-1]
        loc_id = random.choice(range(len(self.config.locations)))
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
            self.config.locations[self.target_location],
            f"192.168.{self.target_location}.{start}",
            f"192.168.{self.target_location}.{end}",
        ]
        for x in range(len(self.config.locations)):
            if x == self.target_location:
                yield target_subnet_entry
                continue
            start = random.randint(0, 255)
            end = random.randint(start, 255)
            yield [
                x,
                self.config.locations[x],
                f"192.168.{x}.{start}",
                f"192.168.{x}.{end}",
            ]

def gen_db(dbidx: int) -> None:
    # Define default schemas
    schemas = [
        ["time", "ip", "end_point"],  # access schema
        ["time", "person_name", "location_id"],  # people schema
        ["location_id", "location_name", "subnet_start", "subnet_end"]  # subnet schema
    ]
    
    # Create default config
    config = MockDBConfig()
    
    with MockDB(dbidx, config, schemas).with_writer(JSONFileWriter) as mdb:
        mdb.init_tables().write_tables().log_answer()

if __name__ == "__main__":
    gen_db(0)