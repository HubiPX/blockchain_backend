import random
import time
from flask import Blueprint, session, request, current_app
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker
from database.models import db
from database.models import Users, TransactionsMySQL, TransactionsSQLite, TransactionsMongo
from blueprints.auth import Auth
from datetime import datetime
import hashlib

transactions = Blueprint('transactions', __name__)


@transactions.route('/transfer-score', methods=['POST'])
@Auth.logged_user
def transfer_score():
    from main import mongo
    data = request.get_json()

    recipient_username = data.get('recipient')
    amount = data.get('amount')

    # Walidacja danych
    if not recipient_username or amount is None:
        return 'Brak danych: odbiorcy lub ilości punktów.', 400

    try:
        amount = int(amount)
    except ValueError:
        return 'Ilość punktów musi być liczbą całkowitą.', 400

    if amount <= 0:
        return 'Ilość punktów musi być większa niż zero.', 400

    sender = Users.query.filter_by(id=session["user_id"]).first()
    recipient = Users.query.filter_by(username=recipient_username).first()

    if recipient_username == sender.username:
        return 'Nie możesz wysłać punktów do siebie.', 400

    if not recipient:
        return 'Użytkownik odbierający nie istnieje.', 404

    if sender.score < amount:
        return 'Nie masz wystarczającej liczby punktów do przesłania.', 400

    # Transfer punktów
    sender.score -= amount
    recipient.score += amount

    now = datetime.utcnow()
    tx_data = {
        'sender': sender.username,
        'recipient': recipient.username,
        'amount': amount,
        'date': now
    }

    tx_mysql = TransactionsMySQL(**tx_data)
    tx_sqlite = TransactionsSQLite(**tx_data)
    transactions_mongo = TransactionsMongo(mongo)

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

        # Zapis do MySQL
        current_app.blockchains["mysql"].hm_add_transaction_to_mempool(tx)  # type: ignore

        # Zapis do SQLite
        current_app.blockchains["sqlite"].hm_add_transaction_to_mempool(tx)  # type: ignore

        # Zapis do Mongo
        current_app.blockchains["mongo"].hm_add_transaction_to_mempool(tx)  # type: ignore

        return f'Pomyślnie przesłano {amount} punktów do {recipient_username}.', 200

    except SQLAlchemyError as e:
        db.session.rollback()
        return f'Wystąpił błąd przy zapisie transakcji: {str(e)}', 500
    except Exception as e:
        return f'Wystąpił nieoczekiwany błąd: {str(e)}', 500


@transactions.route('/generate-random-transactions', methods=['POST'])
@Auth.logged_rcon
def generate_random_transactions():
    from main import mongo
    from flask import current_app

    data = request.get_json()
    count = data.get("count")

    if not isinstance(count, int) or count <= 0:
        return "Nieprawidłowa liczba transakcji.", 400

    max_transactions = 10000
    if count > max_transactions:
        return f"Maksymalna dozwolona liczba transakcji to {max_transactions}.", 400

    all_users = Users.query.all()
    if len(all_users) < 2:
        return "Za mało użytkowników do wykonania transakcji.", 400

    user_scores = {user.username: user.score for user in all_users}

    transactions_data = []
    generated = 0
    attempts = 0
    max_attempts = count * 10

    # generowanie losowych transakcji (bez pola 'code')
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

        now = datetime.utcnow()
        tx = {
            'sender': sender_name,
            'recipient': recipient_name,
            'amount': amount,
            'date': now
        }
        transactions_data.append(tx)
        generated += 1

    # przygotowanie sqlite session (jak wcześniej)
    sqlite_engine = db.get_engine(bind='sqlite_db')
    sqlite_session_factory = sessionmaker(bind=sqlite_engine)
    sqlite_session = scoped_session(sqlite_session_factory)

    batch_size = 2000

    try:
        # ---------------------------
        #  HISTORY: zapis do tabel historycznych (MySQL, SQLite, Mongo)
        # ---------------------------
        # MySQL
        start_mysql = time.perf_counter()
        for i in range(0, generated, batch_size):
            batch = transactions_data[i:i + batch_size]
            batch_objects = [TransactionsMySQL(**tx) for tx in batch]
            db.session.add_all(batch_objects)
            db.session.commit()
        end_mysql = time.perf_counter()
        mysql_time = end_mysql - start_mysql

        # SQLite
        start_sqlite = time.perf_counter()
        for i in range(0, generated, batch_size):
            batch = transactions_data[i:i + batch_size]
            batch_objects = [TransactionsSQLite(**tx) for tx in batch]
            sqlite_session.add_all(batch_objects)
            sqlite_session.commit()
        end_sqlite = time.perf_counter()
        sqlite_time = end_sqlite - start_sqlite

        # Mongo (history collection)
        start_mongo = time.perf_counter()
        for i in range(0, generated, batch_size):
            batch = transactions_data[i:i + batch_size]
            mongo_batch = [{
                'sender': tx['sender'],
                'recipient': tx['recipient'],
                'amount': tx['amount'],
                'date': tx['date']
            } for tx in batch]
            mongo.db.transactions.insert_many(mongo_batch)
        end_mongo = time.perf_counter()
        mongo_time = end_mongo - start_mongo

        # ---------------------------
        #  MEMPOOL: dodajemy do blockchainów (batched)
        # ---------------------------
        # przygotuj helper do przygotowania batchu dla mempool (ten sam co history, tylko bez dodatkowych pól)
        def _mempool_batch(batch):
            return [{'sender': tx['sender'], 'recipient': tx['recipient'], 'amount': tx['amount'], 'date': tx['date']} for tx in batch]

        # Dodajemy transakcje do mempooli batched — hm_add_transaction_to_mempool ma obsłużyć tworzenie bloków
        for i in range(0, generated, batch_size):
            batch = transactions_data[i:i + batch_size]
            mem_batch = _mempool_batch(batch)

            # MySQL mempool
            current_app.blockchains["mysql"].hm_add_transaction_to_mempool(mem_batch)  # type: ignore

            # SQLite mempool
            current_app.blockchains["sqlite"].hm_add_transaction_to_mempool(mem_batch)  # type: ignore

            # Mongo mempool
            current_app.blockchains["mongo"].hm_add_transaction_to_mempool(mem_batch)  # type: ignore

        # ---------------------------
        #  Aktualizacja score userów w DB (historyczny Users table)
        # ---------------------------
        for user in all_users:
            user.score = user_scores[user.username]
        db.session.commit()

        return (f"Wygenerowano i dodano {generated} losowych transakcji.\n"
                f"Czasy zapisu:\n"
                f"- MySQL: {mysql_time:.3f} s\n"
                f"- SQLite: {sqlite_time:.3f} s\n"
                f"- MongoDB: {mongo_time:.3f} s"), 200

    except Exception as e:
        db.session.rollback()
        sqlite_session.rollback()
        return f"Błąd przy zapisie transakcji: {str(e)}", 500

    finally:
        sqlite_session.remove()
