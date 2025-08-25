from datetime import datetime
from blockchain.blockchain_base import BlockchainBase


class BlockchainMongo(BlockchainBase):
    def __init__(self, mongo):
        self.mongo = mongo
        super().__init__()

    def get_last_block_from_db(self):
        last_block = self.mongo.db.blockchain_blocks.find().sort("index", -1).limit(1)
        last_block = list(last_block)

        if not last_block:
            return None

        lb = last_block[0]
        print(f"Mongo Last block loaded: index {lb['index']}")

        # Pobranie transakcji powiązanych z tym blokiem
        txs = self.mongo.db.blockchain_transactions.find(
            {"block_id": lb["_id"]}
        ).sort("_id", 1)

        block_dict = {
            "index": lb["index"],
            "timestamp": lb["timestamp"],
            "transactions": [
                {
                    "_id": tx["_id"],
                    "sender": tx["sender"],
                    "recipient": tx["recipient"],
                    "amount": tx["amount"],
                    "date": tx["date"]
                }
                for tx in txs
            ],
            "proof": lb["proof"],
            "previous_hash": lb["previous_hash"],
            "merkle_root": lb["merkle_root"]
        }

        return block_dict

    def save_block_to_db(self, block, transactions):
        if not transactions:
            transactions = []

        db_block = {
            'index': block['index'],
            'timestamp': block['timestamp'],
            'proof': block['proof'],
            'previous_hash': block['previous_hash'],
            'merkle_root': block['merkle_root']
        }
        result = self.mongo.db.blockchain_blocks.insert_one(db_block)
        block_id = result.inserted_id

        db_txs = [
            {
                '_id': tx['_id'],
                'block_id': block_id,
                'sender': tx['sender'],
                'recipient': tx['recipient'],
                'amount': tx['amount'],
                'date': tx['date']
            }
            for tx in transactions
        ]

        self.mongo.db.blockchain_transactions.insert_many(db_txs)

    def save_transactions_to_mempool(self, transactions):
        if not transactions:
            return

        self.mongo.db.mempool_transactions.insert_many(transactions)

    # --- Nowe metody wymagane przez BlockchainBase ---
    def get_pending_transactions(self, limit):
        txs = self.mongo.db.mempool_transactions.find().sort('date', 1).limit(limit)
        return [
            {'_id': tx['_id'], 'sender': tx['sender'], 'recipient': tx['recipient'], 'amount': tx['amount'], 'date': tx['date']}
            for tx in txs
        ]

    def get_mempool_count(self):
        return self.mongo.db.mempool_transactions.count_documents({})

    def clear_pending_transactions(self, transactions):
        if not transactions:
            return
        ids = [tx['_id'] for tx in transactions]
        self.mongo.db.mempool_transactions.delete_many({'_id': {'$in': ids}})

    def get_full_chain(self) -> list[dict]:
        """Zwraca cały blockchain w kolejności rosnącej po index,
           z transakcjami w odpowiednim miejscu w bloku"""

        blocks = self.mongo.db.blockchain_blocks.find().sort("index", 1)

        chain = []
        for block in blocks:
            txs = self.mongo.db.blockchain_transactions.find(
                {"block_id": block["_id"]}
            ).sort("_id", 1)

            block_dict = {
                "index": block["index"],
                "timestamp": block["timestamp"],
                "transactions": [
                    {
                        "_id": tx["_id"],
                        "sender": tx["sender"],
                        "recipient": tx["recipient"],
                        "amount": tx["amount"],
                        "date": tx["date"]
                    }
                    for tx in txs
                ],
                "proof": block["proof"],
                "previous_hash": block["previous_hash"],
                "merkle_root": block["merkle_root"]
            }

            chain.append(block_dict)

        return chain

