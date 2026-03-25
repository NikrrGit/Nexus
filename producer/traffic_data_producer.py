from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
from datetime import datetime,timedelta 
import pytz

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

roads = ["R100", "R200", "R300", "R400"]
zones = ["CBD", "AIRPORT", "TECHPARK", "SUBURB", "TRAINSTATION"]
weather = ["CLEAR", "RAIN", "FOG", "STORM"]

vehicle_cache = []

def generate_clean_event():

    vid = fake.uuid4()
    vehicle_cache.append(vid)

    return {
        "vehicle_id": vid,
        "road_id": random.choice(roads),
        "city_zone": random.choice(zones),
        "speed": random.randint(20, 100),
        "congestion_level": random.randint(1, 5),
        "weather": random.choice(weather),
        "event_time": datetime.now(pytz.utc).isoformat()
    }


def generate_dirty_event():

    dirty_type = random.choice([
        "null_speed",
        "negative_speed",
        "extreme_speed",
        "duplicate_vehicle",
        "late_event",
        "future_event",
        "wrong_datatype",
        "schema_drift",
        "corrupt_json"
    ])

    base = generate_clean_event()

    if dirty_type == "null_speed":
        base["speed"] = None

    elif dirty_type == "negative_speed":
        base["speed"] = -40

    elif dirty_type == "extreme_speed":
        base["speed"] = 420

    elif dirty_type == "duplicate_vehicle" and vehicle_cache:
        base["vehicle_id"] = random.choice(vehicle_cache)

    elif dirty_type == "late_event":
        base["event_time"] = (
            datetime.now(pytz.utc) - timedelta(minutes=random.randint(10, 120))
        ).isoformat()

    elif dirty_type == "future_event":
        base["event_time"] = (
            datetime.now(pytz.utc) + timedelta(minutes=random.randint(5, 60))
        ).isoformat()

    elif dirty_type == "wrong_datatype":
        base["speed"] = "FAST"

    elif dirty_type == "schema_drift":
        base["road_condition"] = random.choice(["GOOD", "BAD", "UNDER_CONSTRUCTION"])

    elif dirty_type == "corrupt_json":
        return "###CORRUPTED_EVENT###"

    return base


while True:

    if random.random() < 0.7:
        event = generate_clean_event()
    else:
        event = generate_dirty_event()

    if isinstance(event, str):
        producer.send("traffic-topic", value={"raw": event})
        print("CORRUPT EVENT SENT")
    else:
        producer.send("traffic-topic", value=event)
        print(event)

    time.sleep(random.uniform(0.5, 1.5))