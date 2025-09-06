import random
import time
from flask import Blueprint, session, request, current_app, jsonify
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker
from database.models import db, MempoolTransactionMySQL
from database.models import Users, TransactionsMySQL, TransactionsSQLite, TransactionsMongo
from blueprints.auth import Auth
from datetime import datetime, timedelta
import os

transactions = Blueprint('transactions', __name__)


def generate_transactions(count, user_scores, all_users):
    """
    Generuje listę transakcji między użytkownikami.

    :param count: liczba transakcji do wygenerowania
    :param user_scores: słownik {username: score}
    :param all_users: lista obiektów użytkowników (muszą mieć .username)
    :return: lista transakcji w formacie {'sender', 'recipient', 'amount', 'date'}
    """
    transactions_data = []
    generated = 0
    attempts = 0
    max_attempts = count * 10
    last_time = None

    while generated < count and attempts < max_attempts:
        attempts += 1
        sender, recipient = random.sample(all_users, 2)
        sender_name = sender.username
        recipient_name = recipient.username

        if user_scores[sender_name] <= 0:
            continue

        amount = random.randint(1, min(500, user_scores[sender_name]))

        if user_scores[sender_name] - amount < 0:
            continue

        user_scores[sender_name] -= amount
        user_scores[recipient_name] += amount

        now = datetime.now().replace(microsecond=(datetime.now().microsecond // 1000) * 1000)

        if last_time and now <= last_time:
            now = last_time + timedelta(milliseconds=1)

        last_time = now

        tx = {
            'sender': sender_name,
            'recipient': recipient_name,
            'amount': amount,
            'date': now
        }
        transactions_data.append(tx)
        generated += 1

    return transactions_data


@transactions.route('/transfer-score', methods=['POST'])
@Auth.logged_user
def transfer_score():
    data = request.get_json()

    recipient_username = data.get('recipient')
    amount = data.get('amount')

    # Walidacja danych
    if not recipient_username or amount is None:
        return jsonify({"message": "Brak danych: odbiorcy lub ilości punktów."}), 400

    try:
        amount = int(amount)
    except ValueError:
        return jsonify({"message": "Ilość punktów musi być liczbą całkowitą."}), 400

    if amount <= 0:
        return jsonify({"message": "Ilość punktów musi być większa niż zero."}), 400

    sender = Users.query.filter_by(id=session["user_id"]).first()
    recipient = Users.query.filter_by(username=recipient_username).first()

    if recipient_username == sender.username:
        return jsonify({"message": "Nie możesz wysłać punktów do siebie."}), 400

    if not recipient:
        return jsonify({"message": "Użytkownik odbierający nie istnieje."}), 404

    if sender.score < amount:
        return jsonify({"message": "Nie masz wystarczającej liczby punktów do przesłania."}), 400

    # Transfer punktów
    sender.score -= amount
    recipient.score += amount

    now = datetime.now().replace(microsecond=(datetime.now().microsecond // 1000) * 1000)

    tx_data = {
        'sender': sender.username,
        'recipient': recipient.username,
        'amount': amount,
        'date': now
    }

    tx_mysql = TransactionsMySQL(**tx_data)
    tx_sqlite = TransactionsSQLite(**tx_data)
    transactions_mongo = TransactionsMongo(current_app.mongo)  # type: ignore

    try:
        # Zapis do MySQL
        db.session.add(tx_mysql)
        db.session.commit()
        # Zapis do SQLite
        sqlite_session = scoped_session(sessionmaker(bind=db.get_engine(bind='sqlite_db')))
        sqlite_session.add(tx_sqlite)
        sqlite_session.commit()
        sqlite_session.remove()
        # Zapis do MongoDB
        transactions_mongo.insert_transaction(
            sender=tx_data['sender'],
            recipient=tx_data['recipient'],
            amount=tx_data['amount'],
            date=tx_data['date']
        )

        # BLOCKCHAIN
        tx = [{
            "sender": sender.username,
            "recipient": recipient.username,
            "amount": amount,
            "date": now
        }]

        mempool_size = 30

        # Zapis do MySQL
        current_app.blockchains["mysql"].hm_add_transaction_to_mempool(tx, mempool_size)  # type: ignore

        # Zapis do SQLite
        current_app.blockchains["sqlite"].hm_add_transaction_to_mempool(tx, mempool_size)  # type: ignore

        # Zapis do Mongo
        current_app.blockchains["mongo"].hm_add_transaction_to_mempool(tx, mempool_size)  # type: ignore

        return jsonify({"message": f"Pomyślnie przesłano {amount} punktów do {recipient_username}."}), 200

    except SQLAlchemyError as e:
        db.session.rollback()
        return jsonify({"message": f"Wystąpił błąd przy zapisie transakcji: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"message": f"Wystąpił nieoczekiwany błąd: {str(e)}"}), 500


@transactions.route('/generate-random-transactions', methods=['POST'])
@Auth.logged_rcon
def generate_random_transactions():
    data = request.get_json()
    count = data.get("count")
    tx_limit = data.get("tx_limit", 30)
    mempool = MempoolTransactionMySQL.query.count()

    if mempool > tx_limit:
        return jsonify({
            "message": f"Limit ({tx_limit}) musi być większy niż aktualna liczba transakcji w mempoolu ({mempool})."
        }), 400

    if not isinstance(count, int) or count <= 0:
        return jsonify({"message": "Nieprawidłowa liczba transakcji."}), 400

    max_transactions = 10000
    if count > max_transactions:
        return jsonify({"message": f"Maksymalna dozwolona liczba transakcji to {max_transactions}."}), 400

    all_users = Users.query.all()
    if len(all_users) < 2:
        return jsonify({"message": "Za mało użytkowników do wykonania transakcji."}), 400

    user_scores = {user.username: user.score for user in all_users}
    transactions_data = generate_transactions(count=count, user_scores=user_scores, all_users=all_users)

    copy_transactions_data_sqlite = [dict(tx) for tx in transactions_data]
    copy_transactions_data_mongo = [dict(tx) for tx in transactions_data]

    # ===========================
    # Pomocnicze funkcje do rozmiaru
    # ===========================

    def get_mysql_size_kb(num_rows, table_names):
        """
        Zwraca szacowany rozmiar dodanych wierszy w KB dla jednej lub wielu tabel.

        :param num_rows: liczba dodanych wierszy dla każdej tabeli
        :param table_names: lista nazw tabel lub pojedyncza nazwa tabeli (str)
        :return: szacowany rozmiar w KB
        """
        # Przybliżone rozmiary w B na wiersz
        table_sizes = {
            "transactions": 76,
            "blockchain_blocks": 168,
            "blockchain_transactions": 80,
            "mempool_transactions": 80,
        }

        # Jeśli przekazano pojedynczą tabelę jako string
        if isinstance(table_names, str):
            table_names = [table_names]

        total_kb = 0.0
        for table in table_names:
            size_per_row = table_sizes.get(table, 100)
            total_kb += (size_per_row * num_rows) / 1024.0

        return total_kb

    def get_sqlite_size_kb(path):
        return os.path.getsize(path)/1024.0 if os.path.exists(path) else 0.0

    def get_mongo_size_kb(db, collections):
        total = 0.0
        for coll in collections:
            try:
                stats = db.command("collstats", coll)
                total += stats.get("size", 0)/1024.0
            except Exception:
                pass
        return total

    mysql_tx_table = ["transactions"]
    mysql_bc_tables = ["blockchain_blocks", "blockchain_transactions", "mempool_transactions"]
    sqlite_db_path = "database/database.db"
    mongo_tx_collection = ["transactions"]
    mongo_bc_collections = ["blockchain_blocks", "blockchain_transactions", "mempool_transactions"]

    # przygotowanie sqlite session
    sqlite_engine = db.get_engine(bind='sqlite_db')
    sqlite_session_factory = sessionmaker(bind=sqlite_engine)
    sqlite_session = scoped_session(sqlite_session_factory)

    try:
        # ===========================
        # Pomiary przed dodaniem danych
        # ===========================
        sqlite_tx_size_before = get_sqlite_size_kb(sqlite_db_path)
        mongo_tx_size_before = get_mongo_size_kb(current_app.mongo.db, mongo_tx_collection)  # type: ignore

        # ===========================
        #  zapis do baz danych (MySQL, SQLite, Mongo)
        # ===========================
        batch_size = 2000

        # MySQL transactions
        start_mysql = time.perf_counter()
        for i in range(0, count, batch_size):
            batch = transactions_data[i:i + batch_size]
            db.session.bulk_insert_mappings(TransactionsMySQL, batch)
            db.session.commit()
        end_mysql = time.perf_counter()
        mysql_time = end_mysql - start_mysql

        # SQLite transactions
        start_sqlite = time.perf_counter()
        for i in range(0, count, batch_size):
            batch = copy_transactions_data_sqlite[i:i + batch_size]
            sqlite_session.bulk_insert_mappings(TransactionsSQLite, batch)  # type: ignore
            sqlite_session.commit()
        end_sqlite = time.perf_counter()
        sqlite_time = end_sqlite - start_sqlite

        # Mongo transactions
        start_mongo = time.perf_counter()
        for i in range(0, count, batch_size):
            batch = copy_transactions_data_mongo[i:i + batch_size]
            current_app.mongo.db.transactions.insert_many(batch)  # type: ignore
        end_mongo = time.perf_counter()
        mongo_time = end_mongo - start_mongo

        # ===========================
        # Pomiary po dodaniu transactions
        # ===========================
        mysql_tx_size_after = get_mysql_size_kb(count, mysql_tx_table)
        sqlite_tx_size_after = get_sqlite_size_kb(sqlite_db_path)
        mongo_tx_size_after = get_mongo_size_kb(current_app.mongo.db, mongo_tx_collection)  # type: ignore

        # ===========================
        #  MEMPOOL: dodajemy do blockchainów (batched)
        # ===========================
        # MySQL blockchain
        sqlite_bc_size_after = get_sqlite_size_kb(sqlite_db_path)
        mongo_bc_size_after = get_mongo_size_kb(current_app.mongo.db, mongo_bc_collections)  # type: ignore

        start_mysql_blockchain = time.perf_counter()
        for i in range(0, count, batch_size):
            batch = transactions_data[i:i + batch_size]
            current_app.blockchains["mysql"].hm_add_transaction_to_mempool(batch, tx_limit)  # type: ignore
        end_mysql_blockchain = time.perf_counter()
        mysql_blockchain_time = end_mysql_blockchain - start_mysql_blockchain

        # SQLite blockchain
        start_sqlite_blockchain = time.perf_counter()
        for i in range(0, count, batch_size):
            batch = copy_transactions_data_sqlite[i:i + batch_size]
            current_app.blockchains["sqlite"].hm_add_transaction_to_mempool(batch, tx_limit)  # type: ignore
        end_sqlite_blockchain = time.perf_counter()
        sqlite_blockchain_time = end_sqlite_blockchain - start_sqlite_blockchain

        # Mongo blockchain
        start_mongo_blockchain = time.perf_counter()
        for i in range(0, count, batch_size):
            batch = copy_transactions_data_mongo[i:i + batch_size]
            current_app.blockchains["mongo"].hm_add_transaction_to_mempool(batch, tx_limit)  # type: ignore
        end_mongo_blockchain = time.perf_counter()
        mongo_blockchain_time = end_mongo_blockchain - start_mongo_blockchain


        # ===========================
        # Pomiary po dodaniu blockchain
        # ===========================
        mysql_bc_size_final = get_mysql_size_kb(count, mysql_bc_tables)
        sqlite_bc_size_final = get_sqlite_size_kb(sqlite_db_path)
        mongo_bc_size_final = get_mongo_size_kb(current_app.mongo.db, mongo_bc_collections)  # type: ignore

        # ===========================
        #  Aktualizacja score userów
        # ===========================
        for user in all_users:
            user.score = user_scores[user.username]
        db.session.commit()

        mempool = MempoolTransactionMySQL.query.count()

        return jsonify({
            "message": f"Wygenerowano i dodano {count} losowych transakcji. \n Mempool: {mempool}/{tx_limit}",
            "db_times": {
                "MySQL": f"{mysql_time:.3f} s",
                "SQLite": f"{sqlite_time:.3f} s",
                "MongoDB": f"{mongo_time:.3f} s",
            },
            "blockchain_times": {
                "MySQL Blockchain": f"{mysql_blockchain_time:.3f} s",
                "SQLite Blockchain": f"{sqlite_blockchain_time:.3f} s",
                "MongoDB Blockchain": f"{mongo_blockchain_time:.3f} s",
            },
            "db_sizes": {
                "MySQL": f"{mysql_tx_size_after:.2f} KB",
                "SQLite": f"{sqlite_tx_size_after - sqlite_tx_size_before:.2f} KB",
                "MongoDB": f"{mongo_tx_size_after - mongo_tx_size_before:.2f} KB"
            },
            "blockchain_sizes": {
                "MySQL": f"{mysql_bc_size_final:.2f} KB",
                "SQLite": f"{sqlite_bc_size_final - sqlite_bc_size_after:.2f} KB",
                "MongoDB": f"{mongo_bc_size_final - mongo_bc_size_after:.2f} KB"
            }
        }), 200

    except Exception as e:
        db.session.rollback()
        sqlite_session.rollback()
        return jsonify({"message": f"Błąd przy zapisie transakcji: {str(e)}"}), 500

    finally:
        sqlite_session.remove()


@transactions.route('/validate', methods=["POST"])
@Auth.logged_admin
def validate_blockchains():
    data = request.get_json()
    blockchain_name = data.get("blockchain_name")

    results = {}
    all_valid = True

    if blockchain_name:
        # Sprawdzamy tylko wybrany blockchain
        blockchain = current_app.blockchains.get(blockchain_name)  # type: ignore
        if blockchain is None:
            return jsonify({"message": f"Blockchain '{blockchain_name}' not found"}), 404

        is_valid, message = blockchain.validate_chain()
        results[blockchain_name] = {
            "valid": is_valid,
            "message": blockchain_name.upper() + " " + message
        }
        all_valid = is_valid
    else:
        # Sprawdzamy wszystkie blockchainy (dotychczasowa logika)
        for name, blockchain in current_app.blockchains.items():  # type: ignore
            is_valid, message = blockchain.validate_chain()
            results[name] = {
                "valid": is_valid,
                "message": name.upper() + " " + message
            }
            if not is_valid:
                all_valid = False

    if all_valid:
        return jsonify({"status": "ok", "message": results}), 200
    else:
        return jsonify({"status": "error", "message": results}), 400


@transactions.route('/merkle_tree', methods=["POST"])
@Auth.logged_admin
def check_merkle_tree():
    data = request.get_json()
    blockchain_name = data.get("blockchain_name")
    block_index = data.get("block_index")
    tx_id = data.get("tx_id")

    if not blockchain_name or block_index is None or tx_id is None:
        return jsonify({"message": 'Brak danych: nazwy blockchainu, id bloku lub id transakcji.'}), 404

    blockchain = current_app.blockchains.get(blockchain_name)  # type: ignore
    if blockchain is None:
        return jsonify({"message": f'Nie znaleziono Blockchainu {blockchain_name}.'}), 404

    # pobierz dowód Merkle dla transakcji
    proof_data = blockchain.get_transaction_proof(block_index=block_index, tx_id=tx_id)
    if not proof_data:
        return jsonify({"message": f'Nie znaleziono transakcji {tx_id} w bloku {block_index}.'}), 404

    # weryfikacja dowodu
    result = blockchain.verify_merkle_proof(
        transaction=proof_data["transaction"],
        proof=proof_data["proof"],
        merkle_root=proof_data["merkle_root"]
    )

    if result:
        return jsonify({"message": f'Transakcja {tx_id} jest poprawna w bloku {block_index}.'}), 200
    else:
        return jsonify({
            "message": f'Drzewo Merkla NIE jest prawidłowe dla transakcji {tx_id} w bloku {block_index}.'
        }), 400

