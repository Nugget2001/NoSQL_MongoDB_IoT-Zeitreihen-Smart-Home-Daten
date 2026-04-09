import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.database.mongodb_client import MongoDBClient

load_dotenv()

def run_benchmark_analytics():
    db = MongoDBClient().get_database()
    collection = db[os.getenv("COLLECTION_NAME", "sensor_data")]

    query_time = datetime.utcnow() - timedelta(minutes=60)

    pipeline = [
        {"$match": {"timestamp": {"$gte": query_time}}},
        {"$group": {
            "_id": "$metadata.room",
            "avg_temp": {"$avg": "$temperature"},
            "avg_hum": {"$avg": "$humidity"},
            "data_points": {"$count": {}}
        }},
        {"$sort": {"_id": 1}}
    ]

    print(f"\n--- BENCHMARK-ANALYSE ({datetime.now().strftime('%H:%M:%S')}) ---")
    print(f"Betrachteter Zeitraum: ab {query_time.isoformat()} UTC")

    start = time.perf_counter()
    results = list(collection.aggregate(pipeline))
    duration = time.perf_counter() - start

    total_points = sum(r.get("data_points", 0) for r in results)

    print(f"Aggregation dauerte: {duration:.4f} Sekunden")
    print(f"Aggregierte Datenpunkte insgesamt: {total_points}")

    if not results:
        print("Keine Daten im gewählten Zeitraum gefunden.")
        return

    for res in results:
        print(
            f"Raum: {res['_id']:<12} | "
            f"Ø Temp: {res['avg_temp']:>5.2f}°C | "
            f"Ø Feuchte: {res['avg_hum']:>5.2f}% | "
            f"Messwerte: {res['data_points']}"
        )

if __name__ == "__main__":
    run_benchmark_analytics()