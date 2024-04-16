import os
from confluent_kafka import SerializingProducer
import simplejson
from datetime import datetime
import random


LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

LATITUDE_INCREMENT = (
    BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES["latitude"]
) / 100
LONGITUDE_INCREMENT = (
    BIRMINGHAM_COORDINATES["longitude"] - LONDON_COORDINATES["longitude"]
) / 100


# Environmental variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def simulate_vehicle_movement():
    global start_location

    start_location["latitude"] += LATITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)
    start_location["longitude"] += LONGITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)


def generate_vehicle_data(device_id):
    return {
        "id": device_id,
        "type": "car",
        "location": {
            "latitude": start_location["latitude"],
            "longitude": start_location["longitude"],
        },
        "speed": 20,
        "direction": "north",
    }


def simulate_journey(producer, device_id):
    while True:
        # Generate vehicle data
        vehicle_data = generate_vehicle_data(device_id)

        # Generate GPS data
        gps_data = {
            "id": "gps1",
            "location": {
                "latitude": start_location["latitude"],
                "longitude": start_location["longitude"],
            },
            "speed": 20,
            "direction": "north",
        }

        # Generate traffic data
        traffic_data = {
            "id": "traffic1",
            "location": {
                "latitude": start_location["latitude"],
                "longitude": start_location["longitude"],
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "speed": 20,
                "direction": "north",
                "road": "Main Street",
            },
        }


if __name__ == "__main__":
    producer = SerializingProducer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "error_cb": lambda err: print(f"Kafka error: {err}"),
            "key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
            "value.serializer": "org.apache.kafka.common.serialization.StringSerializer",
        }
    )

    try:
        pass
    except KeyboardInterrupt:
        print("Simulation ended by the user")
    except Exception as e:
        print(f"Error: {e}")
