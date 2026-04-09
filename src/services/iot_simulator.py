import time
import random
from datetime import datetime
from src.database.mongodb_client import MongoDBClient

def setup_timeseries():
    db = MongoDBClient().get_database()
    col_name = "sensor_data"
    
    # Prüfen, ob Collection existiert
    if col_name not in db.list_collection_names():
        db.create_collection(
            col_name,
            timeseries={
                'timeField': 'timestamp',
                'metaField': 'metadata',
                'granularity': 'seconds'
            }
        )
        print(f"Time-Series Collection '{col_name}' erstellt.")

def simulate_sensors():
    db = MongoDBClient().get_database()
    collection = db["sensor_data"]
    rooms = ["Wohnzimmer", "Küche", "Bad"]

    print("Starte Simulation... (Strg+C zum Beenden)")
    try:
        while True:
            for room in rooms:
                payload = {
                    "timestamp": datetime.utcnow(),
                    "metadata": {"room": room, "type": "environment"},
                    "temperature": round(random.uniform(18.0, 25.0), 2),
                    "humidity": round(random.uniform(30.0, 60.0), 2)
                }
                collection.insert_one(payload)
            print(f"Daten gesendet um {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(0.001)
    except KeyboardInterrupt:
        print("Simulation gestoppt.")

if __name__ == "__main__":
    setup_timeseries()
    simulate_sensors()