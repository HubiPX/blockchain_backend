import os
from sqlalchemy import create_engine, text
from pymongo import MongoClient


# SQLite
db_path = "database/database.db"

if os.path.exists(db_path):
    os.remove(db_path)
    print(f"Plik {db_path} został usunięty. Baza SQLite zresetowana.")
else:
    print(f"Plik {db_path} nie istnieje. Nic do zresetowania.")

# MySQL
SQLALCHEMY_DATABASE_URI = 'mysql://root:102309Spot@localhost/blockchain'
engine = create_engine(SQLALCHEMY_DATABASE_URI)

# Lista tabel do resetu
tables = [
    "pending_btc_transactions",
    "users",
    "mempool_transactions",
    "blockchain_transactions",
    "blockchain_blocks",
    "transactions"
]

with engine.connect() as conn:
    try:
        # Upewniamy się, że działamy na odpowiedniej bazie
        conn.execute(text("USE blockchain"))
        # Wyłączamy SQL_SAFE_UPDATES
        conn.execute(text("SET SQL_SAFE_UPDATES = 0"))

        # Usunięcie wszystkich rekordów
        for table in tables:
            conn.execute(text(f"DELETE FROM {table}"))
            conn.execute(text(f"ALTER TABLE {table} AUTO_INCREMENT = 1"))

        print("Baza MySQL 'blockchain' została zresetowana.")
    except Exception as e:
        print(f"Błąd podczas resetowania bazy: {e}")


# Mongo

# URI MongoDB
MONGO_URI = "mongodb://localhost:27017/blockchain"

# Połączenie
client = MongoClient(MONGO_URI)
db = client.get_database()  # domyślnie bierze bazę z URI, czyli 'blockchain'

# Kolekcje do wyczyszczenia
collections = [
    "mempool_transactions",
    "blockchain_transactions",
    "blockchain_blocks",
    "transactions"
]

try:
    for coll_name in collections:
        coll = db[coll_name]
        deleted_count = coll.delete_many({}).deleted_count
        print(f"{coll_name}: usunięto {deleted_count} dokumentów")
    print("MongoDB 'blockchain' zostało zresetowane.")
except Exception as e:
    print(f"Błąd podczas resetowania MongoDB: {e}")
finally:
    client.close()
