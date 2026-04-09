import os
import time
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
from src.database.mongodb_client import MongoDBClient

load_dotenv()


def get_collection():
    db = MongoDBClient().get_database()
    collection_name = os.getenv("COLLECTION_NAME", "sensor_data")
    return db[collection_name]


def run_query(collection, minutes=60):
    query_time = datetime.now(UTC) - timedelta(minutes=minutes)

    pipeline = [
        {
            "$match": {
                "timestamp": {"$gte": query_time}
            }
        },
        {
            "$group": {
                "_id": "$metadata.room",
                "avg_temp": {"$avg": "$temperature"},
                "avg_humidity": {"$avg": "$humidity"},
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]

    start = time.perf_counter()
    results = list(collection.aggregate(pipeline))
    duration = time.perf_counter() - start

    return results, duration


def print_results(title, results, duration):
    total = sum(r["count"] for r in results)

    print(f"\n--- {title} ---")
    print(f"Dauer: {duration:.4f} Sekunden")
    print(f"Aggregierte Datenpunkte: {total}")

    for r in results:
        print(
            f"Raum: {r['_id']:<12} | "
            f"Ø Temp: {r['avg_temp']:.2f}°C | "
            f"Ø Feuchte: {r['avg_humidity']:.2f}% | "
            f"Messwerte: {r['count']}"
        )


def drop_timestamp_index(collection):
    indexes = collection.index_information()
    if "timestamp_1" in indexes:
        collection.drop_index("timestamp_1")
        print("Index 'timestamp_1' entfernt.")
    else:
        print("Kein Index 'timestamp_1' vorhanden.")


def create_timestamp_index(collection):
    collection.create_index([("timestamp", 1)])
    print("Index 'timestamp_1' erstellt.")


def main():
    collection = get_collection()

    print("Starte Vergleich mit derselben Abfrage...")
    print("1. Ohne Index")
    print("2. Mit Index auf timestamp")

    # Ohne Index
    drop_timestamp_index(collection)
    results_no_index, duration_no_index = run_query(collection, minutes=60)
    print_results("OHNE INDEX", results_no_index, duration_no_index)

    # Mit Index
    create_timestamp_index(collection)
    results_with_index, duration_with_index = run_query(collection, minutes=60)
    print_results("MIT INDEX", results_with_index, duration_with_index)

    print("\n--- VERGLEICH ---")
    diff = duration_no_index - duration_with_index
    print(f"Zeit ohne Index: {duration_no_index:.4f} s")
    print(f"Zeit mit Index:  {duration_with_index:.4f} s")
    print(f"Differenz:       {diff:.4f} s")

    if duration_with_index > 0:
        speedup = duration_no_index / duration_with_index
        print(f"Beschleunigung:  {speedup:.2f}x")


if __name__ == "__main__":
    main()