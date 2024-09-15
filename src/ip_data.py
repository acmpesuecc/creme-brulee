import csv
import random
from datetime import datetime, timedelta
from typing import Callable, Any, Iterator
import base64
from sqlite3 import Cursor, connect

NUM_ROWS = 100  # Number of rows to generate
DDOS = 10
START_TIME = datetime.now() - timedelta(days=1)
END_TIME = datetime.now()
ACCESS_SCHEMA = ["time", "ip", "end_point"]
PEOPLE_SCHEMA = ["time", "person_name", "location_id"]
SUBNET_SCHEMA = ["location_id", "location_name", "subnet_start", "subnet_end"]


# Define the list of endpoints
endpoints = ["/users", "/leaderboard", "/food", "/dinner", "/register", "/pay"]
locations = [
    "bathroom",
    "seminar-hall-1",
    "seminar-hall-2",
    "canteen-4-floor",
    "canteen-5-floor",
    "canteen-pixel-block",
    "mrd-auditorium",
]

person_names = [
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
    x = random.randint(0, len(locations) - 1)
    y = random.randint(0, 255)
    return f"{192}.{168}.{x}.{y}"


def generate_time(start_time, end_time) -> datetime:
    delta = end_time - start_time
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_time + timedelta(seconds=random_seconds)


def interleave(iters):
    while iters:
        chosen = min(random.binomialvariate(1, 0.7), len(iters) - 1)
        it = iters[chosen]
        try:
            yield next(it)
        except StopIteration:
            iters.remove(it)


class MockDB:
    def __init__(
        self,
        db_cursor: Cursor,
        tablename: str,
        schema: list[str],
        get_record: Callable[[], list[str]],
        num_rows: int = NUM_ROWS,
        targets: list[list[Any]] = [],
    ) -> None:
        self.cursor = db_cursor
        self.tablename = tablename
        self.schema = schema
        self.targets = targets
        self.get_record = get_record
        self.num_rows = num_rows

    def normal_records(self) -> Iterator[list[Any]]:
        for _ in range(self.num_rows):
            yield self.get_record()

    def write_to_db(self) -> None:
        params = ", ".join(["?"] * len(self.schema))
        for record in interleave([iter(self.targets), self.normal_records()]):
            self.cursor.execute(
                f"INSERT INTO {self.tablename} VALUES ({params})",
                list(map(str, record)),
            )

    def write_to_file(self) -> None:
        with open(self.tablename, mode="w") as file:
            writer = csv.writer(file)
            writer.writerow(self.schema)
            for record in interleave([iter(self.targets), self.normal_records()]):
                writer.writerow(record)


def gen_db(dbidx: int):
    con = connect(f"challenge_{dbidx}.db")
    cur = con.cursor()

    access_q = ",".join(map(lambda fi: fi + " TEXT", ACCESS_SCHEMA))
    people_q = ",".join(map(lambda fi: fi + " TEXT", PEOPLE_SCHEMA))
    subnet_q = ",".join(map(lambda fi: fi + " TEXT", SUBNET_SCHEMA))

    cur.execute(f"CREATE TABLE access({access_q})")
    cur.execute(f"CREATE TABLE people({people_q})")
    cur.execute(f"CREATE TABLE subnet({subnet_q})")
    con.commit()

    def access_writer() -> list[Any]:
        time_stamp = generate_time(START_TIME, END_TIME)
        ip = generate_ip()
        endpoint = random.choice(endpoints)
        return [time_stamp, ip, endpoint]

    offender = []
    target_time, target_ip, _ = access_writer()
    _, _, target_location, target_subnet = map(int, target_ip.split("."))
    for i in range(DDOS):
        offender.append(
            [
                target_time + timedelta(seconds=random.randint(1, 15)),
                target_ip,
                random.choice(endpoints),
            ]
        )

    MockDB(
        db_cursor=cur,
        tablename="access",
        schema=ACCESS_SCHEMA,
        get_record=access_writer,
        targets=offender,
    ).write_to_db()

    def people_writer_generator(
        location: int, generator: bool = True
    ) -> Callable[[], list[Any]]:
        def people_writer() -> list[Any]:
            time_stamp = generate_time(START_TIME, END_TIME)
            person = str(base64.b64encode(random.choice(person_names).encode()))[2:-1]
            loc_id = random.choice(range(len(locations)))
            if (
                not generator
                and target_time < time_stamp < target_time + timedelta(seconds=15)
                and loc_id == location
            ):
                time_stamp += timedelta(seconds=30)
            return [time_stamp, person, loc_id]

        return people_writer

    _, target_person, _ = people_writer_generator(0, generator=False)()

    MockDB(
        db_cursor=cur,
        tablename="people",
        schema=PEOPLE_SCHEMA,
        get_record=people_writer_generator(target_location),
        targets=[[target_time, target_person, target_location]],
    ).write_to_db()

    def subnet_writer() -> list[str]:
        id, name = random.choice(list(enumerate(locations)))
        return [str(id), name, generate_ip(), generate_ip()]

    start = random.randint(max(target_subnet - 20, 0), 255)
    end = random.randint(start, 255)
    target_subnet_entry = [
        target_location,
        locations[target_location],
        f"192.168.{target_location}.{start}",
        f"192.168.{target_location}.{end}",
    ]
    subnets: list[list[Any]] = []
    for x in range(len(locations)):
        if x == target_location:
            subnets.append(target_subnet_entry)
            continue
        start = random.randint(0, 255)
        end = random.randint(start, 255)
        subnets.append(
            [
                x,
                locations[x],
                f"192.168.{x}.{start}",
                f"192.168.{x}.{end}",
            ]
        )

    MockDB(
        db_cursor=cur,
        tablename="subnet",
        schema=SUBNET_SCHEMA,
        num_rows=0,
        get_record=lambda: [""],
        targets=subnets,
    ).write_to_db()

    con.commit()
    con.close()
    print(
        f"Database: {dbidx}",
        f"Our offender at {target_time} "
        f"named {base64.decodebytes(target_person.encode())!r} ({target_person}) "
        f"using {target_ip} attacked us "
        f"from location {locations[target_location]} ({target_location})",
    )


if __name__ == "__main__":
    for i in range(80):
        gen_db(i + 1)
