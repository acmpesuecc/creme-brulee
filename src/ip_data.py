import csv
import random
from datetime import datetime, timedelta
import time

# Define the list of endpoints
endpoints = ["/endpoint1", "/endpoint2", "/endpoint3"]


# Function to generate a mock IP address in the format 192.168.x.y
def generate_ip():
    x = random.randint(1, 4)
    y = random.randint(0, 255)
    return f"192.168.{x}.{y:03}"


def generate_time(start_time, end_time):
    delta = end_time - start_time
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_time + timedelta(seconds=random_seconds)


# Generate mock data for the CSV file
num_rows = 1000000  # Number of rows to generate
start_time = datetime.now() - timedelta(days=1)
end_time = datetime.now()

# Write mock data to CSV file
output_file = "mock_ip_data_large.csv"

with open(output_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["time", "ip", "end_point"])

    for _ in range(num_rows):
        time_stamp = generate_time(start_time, end_time)
        ip = generate_ip()
        endpoint = random.choice(endpoints)
        writer.writerow([time_stamp, ip, endpoint])

print(f"{num_rows} rows of mock data generated in {output_file}.")
