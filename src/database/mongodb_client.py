import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

class MongoDBClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBClient, cls).__new__(cls)
            uri = os.getenv("MONGO_URI")
            cls._instance.client = MongoClient(uri)
        return cls._instance

    def get_database(self):
        db_name = os.getenv("DB_NAME")
        if db_name is None:
            raise ValueError("FEHLER: DB_NAME wurde nicht in der .env gefunden! Prüfe den Pfad der .env Datei.")
        return self._instance.client[db_name]