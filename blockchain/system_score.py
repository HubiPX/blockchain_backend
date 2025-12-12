from flask import current_app
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker
from database.models import db
from database.models import TransactionsMySQL, TransactionsSQLite, TransactionsMongo
from datetime import datetime


def add_score_system(score: float, user):
    if score is None:
        raise ValueError("Brak danych: ilości punktów.")

    try:
        score = float(score)
        score = round(score, 8)
    except ValueError:
        raise ValueError("Ilość punktów musi być liczbą.")

    if user.score is None:
        user.score = 0

    # Dodanie punktów
    user.score += score
    user.score = round(user.score, 8)

    now = datetime.now().replace(microsecond=(datetime.now().microsecond // 1000) * 1000)

    tx_data = {
        'sender': "SYSTEM",
        'recipient': user.username,
        'amount': score,
        'date': now
    }

    tx_mysql = TransactionsMySQL(**tx_data)
    tx_sqlite = TransactionsSQLite(**tx_data)
    transactions_mongo = TransactionsMongo(current_app.mongo)  # type: ignore

    try:
        # MySQL
        db.session.add(tx_mysql)
        db.session.commit()

        # SQLite — TRANSAKCJE (sqlite_tx)
        sqlite_tx_engine = db.get_engine(bind='sqlite_tx')
        sqlite_tx_session = scoped_session(sessionmaker(bind=sqlite_tx_engine))

        sqlite_tx_session.add(tx_sqlite)
        sqlite_tx_session.commit()
        sqlite_tx_session.remove()

        # MongoDB
        transactions_mongo.insert_transaction(
            sender=tx_data['sender'],
            recipient=tx_data['recipient'],
            amount=tx_data['amount'],
            date=tx_data['date']
        )

        # Blockchain
        tx = [{
            "sender": "SYSTEM",
            "recipient": user.username,
            "amount": score,
            "date": now
        }]

        mempool_size = 30

        # MySQL blockchain
        current_app.blockchains["mysql"].hm_add_transaction_to_mempool(tx, mempool_size)  # type: ignore
        current_app.blockchains["sqlite"].hm_add_transaction_to_mempool(tx, mempool_size)  # type: ignore
        current_app.blockchains["mongo"].hm_add_transaction_to_mempool(tx, mempool_size)  # type: ignore

        return True

    except SQLAlchemyError as e:
        db.session.rollback()
        raise RuntimeError(f"Błąd przy zapisie transakcji: {str(e)}")

    except Exception as e:
        raise RuntimeError(f"Nieoczekiwany błąd: {str(e)}")
