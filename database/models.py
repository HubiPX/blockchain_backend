from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()


class Users(db.Model):  # MySQL
    id = db.Column(db.Integer, primary_key=True, unique=True)
    username = db.Column(db.String(20), nullable=False, unique=True)
    password = db.Column(db.String(192), nullable=False)
    admin = db.Column(db.Integer, primary_key=False, default=0)
    ban_date = db.Column(db.DateTime, nullable=True)
    score = db.Column(db.Integer, primary_key=False, default=0)
    last_login = db.Column(db.DateTime, nullable=True)
    vip_date = db.Column(db.DateTime, nullable=True)


#  MYSQL

class TransactionsMySQL(db.Model):
    __tablename__ = 'transactions'
    id = db.Column(db.Integer, primary_key=True)
    sender = db.Column(db.String(20), nullable=False)
    recipient = db.Column(db.String(20), nullable=False)
    amount = db.Column(db.Integer, nullable=False)
    date = db.Column(db.DateTime, nullable=False)


class BlockchainBlockMySQL(db.Model):
    __tablename__ = 'blockchain_blocks'
    id = db.Column(db.Integer, primary_key=True)
    index = db.Column(db.Integer)
    timestamp = db.Column(db.DateTime)
    proof = db.Column(db.Integer)
    previous_hash = db.Column(db.String(64))
    merkle_root = db.Column(db.String(64))
    hash = db.Column(db.String(64))


class BlockchainTransactionMySQL(db.Model):
    __tablename__ = 'blockchain_transactions'
    id = db.Column(db.Integer, primary_key=True)
    block_id = db.Column(db.Integer, db.ForeignKey('blockchain_blocks.id'))
    sender = db.Column(db.String(20), nullable=False)
    recipient = db.Column(db.String(20), nullable=False)
    amount = db.Column(db.Integer, nullable=False)
    date = db.Column(db.DateTime, nullable=False)


class MempoolTransactionMySQL(db.Model):
    __tablename__ = 'mempool_transactions'
    id = db.Column(db.Integer, primary_key=True)
    sender = db.Column(db.String(20), nullable=False)
    recipient = db.Column(db.String(20), nullable=False)
    amount = db.Column(db.Integer, nullable=False)
    date = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


#  SQLite


class TransactionsSQLite(db.Model):
    __bind_key__ = 'sqlite_db'
    __tablename__ = 'transactions'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer, primary_key=True)
    sender = db.Column(db.String(20), nullable=False)
    recipient = db.Column(db.String(20), nullable=False)
    amount = db.Column(db.Integer, nullable=False)
    date = db.Column(db.DateTime, nullable=False)


class BlockchainBlockSQLite(db.Model):
    __bind_key__ = 'sqlite_db'
    __tablename__ = 'blockchain_blocks'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer, primary_key=True)
    index = db.Column(db.Integer)
    timestamp = db.Column(db.DateTime)
    proof = db.Column(db.Integer)
    previous_hash = db.Column(db.String(64))
    merkle_root = db.Column(db.String(64))
    hash = db.Column(db.String(64))


class BlockchainTransactionSQLite(db.Model):
    __bind_key__ = 'sqlite_db'
    __tablename__ = 'blockchain_transactions'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer, primary_key=True)
    block_id = db.Column(db.Integer, db.ForeignKey('blockchain_blocks.id'))
    sender = db.Column(db.String(20), nullable=False)
    recipient = db.Column(db.String(20), nullable=False)
    amount = db.Column(db.Integer, nullable=False)
    date = db.Column(db.DateTime, nullable=False)


class MempoolTransactionSQLite(db.Model):
    __bind_key__ = 'sqlite_db'
    __tablename__ = 'mempool_transactions'
    __table_args__ = {'sqlite_autoincrement': True}
    id = db.Column(db.Integer, primary_key=True)
    sender = db.Column(db.String(20), nullable=False)
    recipient = db.Column(db.String(20), nullable=False)
    amount = db.Column(db.Integer, nullable=False)
    date = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


#  MongoDB - bez modelu ORM, przykład prostego wrappera:


class TransactionsMongo:
    def __init__(self, mongo):
        self.collection = mongo.db.transactions

    def insert_transaction(self, sender, recipient, amount, date=None):
        if date is None:
            date = datetime.utcnow()
        tx = {
            "sender": sender,
            "recipient": recipient,
            "amount": amount,
            "date": date
        }
        return self.collection.insert_one(tx)


class BlockchainMongo:
    def __init__(self, mongo):
        self.blocks = mongo.db.blockchain_blocks
        self.transactions = mongo.db.blockchain_transactions
        self.mempool = mongo.db.mempool_transactions

    def insert_block(self, index, timestamp=None, proof=None,
                     previous_hash=None, merkle_root=None, hash=None):
        if timestamp is None:
            timestamp = datetime.utcnow()
        block = {
            "index": index,
            "timestamp": timestamp,
            "proof": proof,
            "previous_hash": previous_hash,
            "merkle_root": merkle_root,
            "hash": hash
        }
        return self.blocks.insert_one(block)

    def insert_transactions(self, transactions_data, block_id=None):
        # transactions_data = lista słowników lub jeden słownik
        if not isinstance(transactions_data, list):
            transactions_data = [transactions_data]

        for tx in transactions_data:
            # jeśli nie ma daty, ustaw domyślną
            if "date" not in tx or tx["date"] is None:
                tx["date"] = datetime.utcnow()
            if block_id is not None:
                tx["block_id"] = block_id
        return self.transactions.insert_many(transactions_data)

    def insert_mempool_transaction(self, sender, recipient, amount, date=None):
        if date is None:
            date = datetime.utcnow()
        tx = {
            "sender": sender,
            "recipient": recipient,
            "amount": amount,
            "date": date
        }
        return self.mempool.insert_one(tx)
