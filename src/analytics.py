import os
from datetime import datetime, timedelta
from src.database.mongodb_client import MongoDBClient
from dotenv import load_dotenv

load_dotenv()

def run_sensor_analytics():
    db = MongoDBClient().get_database()
    collection = db[os.getenv("COLLECTION_NAME")]

    # Zeitraum festlegen: Letzte 5 Minuten
    query_time = datetime.utcnow() - timedelta(minutes=10)

    
    

    # Die Aggregation Pipeline
    pipeline = [
        # 1. Filter: Nur Daten der letzten 5 Minuten
        {"$match": {"timestamp": {"$gte": query_time}}},
        
        # 2. Gruppierung: Nach Raum (aus den Metadaten)
        {"$group": {
            "_id": "$metadata.room",
            "avg_temp": {"$avg": "$temperature"},
            "avg_hum": {"$avg": "$humidity"},
            "data_points": {"$count": {}}
        }},
        
        # 3. Sortierung: Alphabetisch nach Raum
        {"$sort": {"_id": 1}}
    ]

    print(f"\n--- ANALYSE-REPORT ({datetime.now().strftime('%H:%M:%S')}) ---")
    results = list(collection.aggregate(pipeline))
    
    if not results:
        print("Noch nicht genügend Daten für eine Analyse vorhanden.")
    
    for res in results:
        print(f"Raum: {res['_id']:<12} | Ø Temp: {res['avg_temp']:>5.2f}°C | "
              f"Ø Feuchte: {res['avg_hum']:>5.2f}% | (Basis: {res['data_points']} Messwerte)")

if __name__ == "__main__":
    run_sensor_analytics()