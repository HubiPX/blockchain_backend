# check_db_sizes.py

import os
from sqlalchemy import create_engine, text
from pymongo import MongoClient

# ================================
# KONFIGURACJA BAZ
# ================================

# MySQL
MYSQL_URI = 'mysql://root:102309Spot@localhost/blockchain'
mysql_engine = create_engine(MYSQL_URI)

# SQLite
SQLITE_PATH = "database/database.db"

# MongoDB
MONGO_URI = "mongodb://localhost:27017/blockchain"
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client.get_database()

# ================================
# POMIAR ROZMIARU
# ================================


def get_mysql_tables_size_kb(engine, tables):
    with engine.connect() as conn:
        # Najpierw analizujemy tabele, żeby statystyki były aktualne
        for table in tables:
            conn.execute(text(f"ANALYZE TABLE {table}"))

        # Teraz pobieramy rozmiar
        placeholders = ", ".join(f"'{table}'" for table in tables)
        query = text(f"""
            SELECT SUM(data_length + index_length) / 1024.0 AS size_kb
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name IN ({placeholders})
        """)
        result = conn.execute(query).scalar()
        return float(result or 0.0)


def get_sqlite_db_size_kb(sqlite_path):
    if os.path.exists(sqlite_path):
        return os.path.getsize(sqlite_path) / 1024.0
    return 0.0


def get_mongo_collections_size_kb(db, collections):
    total_size = 0.0
    for coll_name in collections:
        try:
            stats = db.command("collstats", coll_name)
            total_size += stats.get("size", 0) / 1024.0
        except Exception:
            pass
    return total_size

# ================================
# LISTA TABEL / KOLEKCJI
# ================================


mysql_tables = [
    "transactions",
    "blockchain_blocks",
    "blockchain_transactions",
    "mempool_transactions"
]

mongo_collections = [
    "transactions",
    "blockchain_blocks",
    "blockchain_transactions",
    "mempool_transactions"
]

# ================================
# POMIAR ROZMIARU
# ================================

if __name__ == "__main__":
    mysql_size = get_mysql_tables_size_kb(mysql_engine, mysql_tables)
    sqlite_size = get_sqlite_db_size_kb(SQLITE_PATH)
    mongo_size = get_mongo_collections_size_kb(mongo_db, mongo_collections)

    print(f"MySQL selected tables size: {mysql_size:.2f} KB")
    print(f"SQLite DB size: {sqlite_size:.2f} KB")
    print(f"MongoDB selected collections size: {mongo_size:.2f} KB")
