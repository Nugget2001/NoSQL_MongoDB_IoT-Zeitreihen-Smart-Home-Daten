import time
import random
from datetime import datetime
from src.database.mongodb_client import MongoDBClient

def setup_timeseries():
    db = MongoDBClient().get_database()
    col_name = "sensor_data"

    if col_name not in db.list_collection_names():
        db.create_collection(
            col_name,
            timeseries={
                "timeField": "timestamp",
                "metaField": "metadata",
                "granularity": "seconds"
            }
        )
        print(f"Time-Series Collection '{col_name}' erstellt.")

def simulate_load(batch_size=10000, sleep_seconds=1.0):
    db = MongoDBClient().get_database()
    collection = db["sensor_data"]
    rooms = ["Wohnzimmer", "Küche", "Bad"]

    total_inserted = 0
    batch_number = 0

    print("Starte Lasttest-Simulation... (Strg+C zum Beenden)")
    print(f"batch_size={batch_size}, sleep_seconds={sleep_seconds}")

    try:
        while True:
            now = datetime.utcnow()
            batch = []

            docs_per_room = batch_size // len(rooms)

            for room in rooms:
                for _ in range(docs_per_room):
                    batch.append({
                        "timestamp": now,
                        "metadata": {"room": room, "type": "environment"},
                        "temperature": round(random.uniform(18.0, 25.0), 2),
                        "humidity": round(random.uniform(30.0, 60.0), 2)
                    })

            # Rest auffüllen
            remainder = batch_size - len(batch)
            for _ in range(remainder):
                room = random.choice(rooms)
                batch.append({
                    "timestamp": now,
                    "metadata": {"room": room, "type": "environment"},
                    "temperature": round(random.uniform(18.0, 25.0), 2),
                    "humidity": round(random.uniform(30.0, 60.0), 2)
                })

            collection.insert_many(batch)

            total_inserted += len(batch)
            batch_number += 1

            print(
                f"Batch {batch_number}: {len(batch)} Datensätze eingefügt | "
                f"Gesamt: {total_inserted}"
            )

            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        print("\nLasttest-Simulation gestoppt.")
        print(f"Insgesamt eingefügt: {total_inserted} Datensätze")

if __name__ == "__main__":
    setup_timeseries()
    simulate_load(batch_size=10000, sleep_seconds=1.0)