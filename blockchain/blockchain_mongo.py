from datetime import datetime
from blockchain.blockchain_base import BlockchainBase


class BlockchainMongo(BlockchainBase):
    def __init__(self, mongo):
        self.mongo = mongo
        super().__init__()

    def get_mempool_from_db(self):
        # Mongo nie wymusza schematu, więc zwracamy listę słowników pasującą do Base
        mempool = self.mongo.db.mempool_transactions.find()
        return [
            {'sender': tx['sender'], 'recipient': tx['recipient'], 'amount': tx['amount'], 'date': tx['date']}
            for tx in mempool
        ]

    def get_last_block_from_db(self):
        last_block = self.mongo.db.blockchain_blocks.find().sort('index', -1).limit(1)
        last_block = list(last_block)
        if last_block:
            lb = last_block[0]
            print(f"Mongo Last block loaded: index {lb['index']}")
            return {
                'index': lb['index'],
                'timestamp': lb['timestamp'],
                'proof': lb['proof'],
                'previous_hash': lb['previous_hash'],
                'merkle_root': lb['merkle_root'],
                'hash': lb['hash']
            }
        return None

    def save_block_to_db(self, block, transactions):
        # W Mongo nie ma autoincrement id, ale możemy Mongo wygenerować _id i przypisać block_id w transakcjach
        db_block = {
            'index': block['index'],
            'timestamp': block['timestamp'],
            'proof': block['proof'],
            'previous_hash': block['previous_hash'],
            'merkle_root': block['merkle_root'],
            'hash': block['hash']
        }
        result = self.mongo.db.blockchain_blocks.insert_one(db_block)
        block_id = result.inserted_id  # Mongo _id

        for tx in transactions:
            db_tx = {
                'block_id': block_id,
                'sender': tx['sender'],
                'recipient': tx['recipient'],
                'amount': tx['amount'],
                'date': tx['date']
            }
            self.mongo.db.blockchain_transactions.insert_one(db_tx)

    def save_transaction_to_mempool(self, sender, recipient, amount, date):
        db_tx = {
            'sender': sender,
            'recipient': recipient,
            'amount': amount,
            'date': date
        }
        self.mongo.db.mempool_transactions.insert_one(db_tx)

    def clear_mempool(self):
        self.mongo.db.mempool_transactions.delete_many({})
