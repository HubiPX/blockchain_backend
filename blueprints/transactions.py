import random
import time
from database.hash import Hash
import requests
from flask import Blueprint, session, request, current_app, jsonify
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker
from database.models import db, MempoolTransactionMySQL, PendingBtcTransactions
from database.models import Users, TransactionsMySQL, TransactionsSQLite, TransactionsMongo
from blueprints.auth import Auth
from datetime import datetime, timedelta
from threading import Thread
from blockchain.system_score import add_score_system

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

        amount = random.randint(1, 1000000) / 1000000

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
        amount = float(amount)
        amount = round(amount, 8)
        print(amount)
    except ValueError:
        return jsonify({"message": "Ilość punktów musi być liczbą."}), 400

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
    batch_size = data.get("batch_size", 1000)

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

    # przygotowanie sqlite session
    sqlite_engine = db.get_engine(bind='sqlite_db')
    sqlite_session_factory = sessionmaker(bind=sqlite_engine)
    sqlite_session = scoped_session(sqlite_session_factory)

    try:
        # ---------------------------
        #  zapis do baz danych (MySQL, SQLite, Mongo)
        # ---------------------------
        # MySQL
        start_mysql = time.perf_counter()
        for i in range(0, count, batch_size):
            batch = transactions_data[i:i + batch_size]
            db.session.bulk_insert_mappings(TransactionsMySQL, batch)
            db.session.commit()
        end_mysql = time.perf_counter()
        mysql_time = end_mysql - start_mysql

        # SQLite
        start_sqlite = time.perf_counter()
        for i in range(0, count, batch_size):
            batch = copy_transactions_data_sqlite[i:i + batch_size]
            sqlite_session.bulk_insert_mappings(TransactionsSQLite, batch)  # type: ignore
            sqlite_session.commit()
        end_sqlite = time.perf_counter()
        sqlite_time = end_sqlite - start_sqlite

        # Mongo (history collection)
        start_mongo = time.perf_counter()
        for i in range(0, count, batch_size):
            batch = copy_transactions_data_mongo[i:i + batch_size]
            current_app.mongo.db.transactions.insert_many(batch)  # type: ignore
        end_mongo = time.perf_counter()
        mongo_time = end_mongo - start_mongo

        # ---------------------------
        #  MEMPOOL: dodajemy do blockchainów (batched) + pomiar czasu
        # ---------------------------

        # MySQL blockchain
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

        # ---------------------------
        #  Aktualizacja score userów w DB (historyczny Users table)
        # ---------------------------
        for user in all_users:
            user.score = round(user_scores[user.username], 8)  # <- zaokrąglenie do 8 miejsc po przecinku
        db.session.commit()

        mempool = MempoolTransactionMySQL.query.count()

        return jsonify({
            "message": f"Wygenerowano i dodano {count} losowych transakcji. \n Mempool: {mempool}/{tx_limit}.",
            "db_times": {
                "MySQL": f"{mysql_time:.3f} s",
                "SQLite": f"{sqlite_time:.3f} s",
                "MongoDB": f"{mongo_time:.3f} s",
            },
            "blockchain_times": {
                "MySQL Blockchain": f"{mysql_blockchain_time:.3f} s",
                "SQLite Blockchain": f"{sqlite_blockchain_time:.3f} s",
                "MongoDB Blockchain": f"{mongo_blockchain_time:.3f} s",
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
    batch_size = data.get("batch_size", 1000)

    results = {}
    all_valid = True

    if str(blockchain_name) in ["mysql", "mongo", "sqlite"]:
        # Sprawdzamy tylko wybrany blockchain
        blockchain = current_app.blockchains.get(blockchain_name)  # type: ignore
        if blockchain is None:
            return jsonify({"message": f"Blockchain '{blockchain_name}' nie istnieje."}), 404

        is_valid, message = blockchain.validate_chain(batch_size=batch_size)
        results[blockchain_name] = {
            "valid": is_valid,
            "message": blockchain_name.upper() + " " + message
        }
        all_valid = is_valid
    elif str(blockchain_name) == "":
        # Sprawdzamy wszystkie blockchainy (dotychczasowa logika)
        for name, blockchain in current_app.blockchains.items():  # type: ignore
            is_valid, message = blockchain.validate_chain(batch_size=batch_size)
            results[name] = {
                "valid": is_valid,
                "message": name.upper() + " " + message
            }
            if not is_valid:
                all_valid = False
    else:
        return jsonify({"message": f"Baza danych o nazwie {blockchain_name} nie istnieje."}), 404
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

    if blockchain_name not in ["mysql", "mongo", "sqlite"]:
        return jsonify({"message": f"Baza danych o nazwie {blockchain_name} nie istnieje."}), 404
    elif block_index is None or tx_id is None:
        return jsonify({"message": 'Brak danych: id bloku lub id transakcji.'}), 404

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


@transactions.route('/check_user_score', methods=["POST"])
@Auth.logged_admin
def check_user_score():
    data = request.get_json()
    username = data.get("username")
    blockchain_name = data.get("blockchain_name")

    if not username or blockchain_name not in ["mysql", "mongo", "sqlite"]:
        return jsonify({"message": "Brak danych: username lub błędna nazwa bazy danych."}), 400

    user = Users.query.filter_by(username=username).first()
    if not user:
        return jsonify({"message": f'Użytkownik {username} nie istnieje.'}), 404

    blockchain = current_app.blockchains.get(blockchain_name)  # type: ignore
    if blockchain is None:
        return jsonify({"message": f'Nie znaleziono Blockchainu {blockchain_name}.'}), 404

    expected_score = blockchain.get_user_score(username)
    actual_score = user.score

    expected_score = round(expected_score, 8)
    actual_score = round(actual_score, 8)

    if expected_score == actual_score:
        return jsonify({"message": f"Score użytkownika {username} jest poprawne - {actual_score}."}), 200
    else:
        difference = actual_score - expected_score

        if difference < 0:
            message = f"Użytkownik {username} ma o {-difference} score za mało."
        else:
            message = f"Użytkownik {username} ma o {difference} score za dużo."

        return jsonify({
            "message": message
        }), 400


def extract_addresses(tx):
    """Pobiera bezpiecznie nadawcę i odbiorcę transakcji."""
    # FROM
    from_addr = "N/A"
    if tx.get("vin"):
        vin0 = tx["vin"][0]
        prevout = vin0.get("prevout")
        if prevout and isinstance(prevout, dict):
            from_addr = prevout.get("scriptpubkey_address", "N/A")
    # TO
    to_addr = "N/A"
    if tx.get("vout"):
        vout0 = tx["vout"][0]
        to_addr = vout0.get("scriptpubkey_address", "N/A")
    return from_addr, to_addr


def get_value_btc(tx):
    """Oblicza sumę wartości wyjść w BTC."""
    total_sats = sum(o.get("value", 0) for o in tx.get("vout", []))
    return total_sats / 1e8


def fetch_btc_transactions_background(app, count):
    with app.app_context():
        saved = 0
        created_users = 0
        try:
            latest = int(requests.get("https://blockstream.info/api/blocks/tip/height").text)
        except:
            return
        ZAKRES_BLOKOW = 50

        while saved < count:
            height = latest - random.randint(1, ZAKRES_BLOKOW)
            try:
                block_hash = requests.get(f"https://blockstream.info/api/block-height/{height}").text.strip()
                txs = requests.get(f"https://blockstream.info/api/block/{block_hash}/txs").json()
            except:
                continue

            random.shuffle(txs)

            for tx in txs:
                if saved >= count:
                    break

                value_btc = get_value_btc(tx)
                if value_btc <= 0:
                    continue

                sender, recipient = extract_addresses(tx)
                if sender == "N/A" or recipient == "N/A":
                    continue

                # Tworzenie kont w Users, jeśli nie istnieją
                for username in [sender, recipient]:
                    if not Users.query.filter_by(username=username).first():
                        hash_pwd = Hash.hash_password(username)
                        new_user = Users(
                            username=username,
                            password=hash_pwd
                        )
                        db.session.add(new_user)
                        created_users += 1
                        add_score_system(100, new_user)
                try:
                    db.session.commit()
                except SQLAlchemyError:
                    db.session.rollback()
                    continue

                # Zapis transakcji BTC
                pending_tx = PendingBtcTransactions(
                    sender=sender,
                    recipient=recipient,
                    amount=value_btc
                )
                try:
                    db.session.add(pending_tx)
                    db.session.commit()
                    saved += 1
                except SQLAlchemyError:
                    db.session.rollback()
                    continue


@transactions.route('/fetch', methods=["POST"])
@Auth.logged_admin
def fetch_btc_transactions():
    data = request.get_json() or {}
    count = data.get("count")
    if count is None:
        return jsonify({"message": "Parametr 'count' jest wymagany."}), 400
    if not isinstance(count, int) or count <= 0:
        return jsonify({"message": "Parametr 'count' musi być liczbą całkowitą większą od 0."}), 400

    existing_count = PendingBtcTransactions.query.count()
    total = existing_count + count

    # Przekazujemy instancję Flaska do wątku
    thread = Thread(target=fetch_btc_transactions_background, args=(current_app._get_current_object(), count))
    thread.start()

    return jsonify({
        "message": f"Rozpoczęto pobieranie {count} nowych transakcji BTC w tle. "
                   f"Po zakończeniu będzie łącznie {total} transakcji."
    }), 200


@transactions.route('/btc_tx', methods=['POST'])
@Auth.logged_rcon
def process_pending_transactions():
    data = request.get_json()
    count = data.get("count")
    tx_limit = data.get("tx_limit", 30)
    batch_size = data.get("batch_size", 1000)

    if not isinstance(count, int) or count <= 0:
        return jsonify({"message": "Nieprawidłowa liczba transakcji."}), 400

    pending_count = PendingBtcTransactions.query.count()
    if pending_count < count:
        missing = count - pending_count
        return jsonify({"message": f"Brakuje {missing} oczekujących transakcji."}), 400

    # Pobranie wszystkich pending transakcji od najniższego ID
    pending_txs = PendingBtcTransactions.query.order_by(PendingBtcTransactions.id.asc()).limit(count).all()

    # Przygotowanie danych w formacie słowników
    transactions_data = [
        {
            "sender": tx.sender,
            "recipient": tx.recipient,
            "amount": tx.amount,
            "date": getattr(tx, "date", datetime.utcnow())
        }
        for tx in pending_txs
    ]

    copy_transactions_data_sqlite = [dict(tx) for tx in transactions_data]
    copy_transactions_data_mongo = [dict(tx) for tx in transactions_data]

    # przygotowanie SQLite session
    sqlite_engine = db.get_engine(bind='sqlite_db')
    sqlite_session_factory = sessionmaker(bind=sqlite_engine)
    sqlite_session = scoped_session(sqlite_session_factory)

    try:
        # ---------------------------
        # MySQL
        # ---------------------------
        start_mysql = time.perf_counter()
        for i in range(0, len(transactions_data), batch_size):
            batch = transactions_data[i:i + batch_size]
            db.session.bulk_insert_mappings(TransactionsMySQL, batch)
            db.session.commit()
        end_mysql = time.perf_counter()
        mysql_time = end_mysql - start_mysql

        # ---------------------------
        # SQLite
        # ---------------------------
        start_sqlite = time.perf_counter()
        for i in range(0, len(copy_transactions_data_sqlite), batch_size):
            batch = copy_transactions_data_sqlite[i:i + batch_size]
            sqlite_session.bulk_insert_mappings(TransactionsSQLite, batch)  # type: ignore
            sqlite_session.commit()
        end_sqlite = time.perf_counter()
        sqlite_time = end_sqlite - start_sqlite

        # ---------------------------
        # MongoDB
        # ---------------------------
        start_mongo = time.perf_counter()
        for i in range(0, count, batch_size):
            batch = copy_transactions_data_mongo[i:i + batch_size]
            current_app.mongo.db.transactions.insert_many(batch)  # type: ignore
        end_mongo = time.perf_counter()
        mongo_time = end_mongo - start_mongo
        # ---------------------------
        # MEMPOOL - blockchainy
        # ---------------------------
        # MySQL blockchain
        start_mysql_blockchain = time.perf_counter()
        for i in range(0, len(transactions_data), batch_size):
            batch = transactions_data[i:i + batch_size]
            current_app.blockchains["mysql"].hm_add_transaction_to_mempool(batch, tx_limit)  # type: ignore
        end_mysql_blockchain = time.perf_counter()
        mysql_blockchain_time = end_mysql_blockchain - start_mysql_blockchain

        # SQLite blockchain
        start_sqlite_blockchain = time.perf_counter()
        for i in range(0, len(copy_transactions_data_sqlite), batch_size):
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
        # ---------------------------
        # Aktualizacja score userów
        # ---------------------------
        all_users = Users.query.all()
        user_scores = {user.username: user.score for user in all_users}

        for tx in transactions_data:
            sender = tx["sender"]
            recipient = tx["recipient"]
            amount = tx["amount"]

            if sender in user_scores:
                user_scores[sender] -= amount

            if recipient in user_scores:
                user_scores[recipient] += amount

        # Aktualizacja w bazie
        for user in all_users:
            user.score = round(user_scores[user.username], 8)  # <- zaokrąglenie do 8 miejsc po przecinku

        db.session.commit()

        # ---------------------------
        # Usuń przetworzone pending transakcje
        # ---------------------------
        for tx in pending_txs:
            db.session.delete(tx)
        db.session.commit()

        mempool = MempoolTransactionMySQL.query.count()

        return jsonify({
            "message": f"Przetworzono {len(transactions_data)} transakcji. Mempool: {mempool}/{tx_limit}.",
            "db_times": {
                "MySQL": f"{mysql_time:.3f} s",
                "SQLite": f"{sqlite_time:.3f} s",
                "MongoDB": f"{mongo_time:.3f} s",
            },
            "blockchain_times": {
                "MySQL Blockchain": f"{mysql_blockchain_time:.3f} s",
                "SQLite Blockchain": f"{sqlite_blockchain_time:.3f} s",
                "MongoDB Blockchain": f"{mongo_blockchain_time:.3f} s",
            }
        }), 200

    except Exception as e:
        db.session.rollback()
        sqlite_session.rollback()
        return jsonify({"message": f"Błąd przy przetwarzaniu transakcji: {str(e)}"}), 500

    finally:
        sqlite_session.remove()
