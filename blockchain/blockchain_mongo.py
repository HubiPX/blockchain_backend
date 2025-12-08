from blockchain.blockchain_base import BlockchainBase
from collections import defaultdict


class BlockchainMongo(BlockchainBase):
    def __init__(self, mongo):
        self.mongo = mongo
        self.blocks = self.mongo.db.blockchain_blocks
        self.transactions = self.mongo.db.blockchain_transactions
        self.mempool = self.mongo.db.mempool_transactions
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

        if db_txs:
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

    def get_chain_batch(self, offset: int, limit: int) -> list[dict]:
        """Pobiera fragment blockchaina z MongoDB w kolejności rosnącej po index."""

        # Pobranie bloków
        blocks = list(
            self.mongo.db.blockchain_blocks
            .find()
            .sort("index", 1)
            .skip(offset)
            .limit(limit)
        )

        if not blocks:
            return []

        # Pobranie wszystkich transakcji dla tych bloków jednym zapytaniem
        block_ids = [block["_id"] for block in blocks]
        all_txs = list(
            self.mongo.db.blockchain_transactions
            .find({"block_id": {"$in": block_ids}})
            .sort("_id", 1)
        )

        # Grupowanie transakcji według block_id
        tx_by_block = defaultdict(list)
        for tx in all_txs:
            tx_by_block[tx["block_id"]].append(tx)

        # Tworzenie listy bloków z transakcjami
        chain = []
        for block in blocks:
            block_dict = {
                "index": block["index"],
                "timestamp": block["timestamp"],
                "transactions": [
                    {
                        "_id": tx["_id"],
                        "sender": tx["sender"],
                        "recipient": tx["recipient"],
                        "amount": tx["amount"],
                        "date": tx["date"],
                    }
                    for tx in tx_by_block[block["_id"]]
                ],
                "proof": block["proof"],
                "previous_hash": block["previous_hash"],
                "merkle_root": block["merkle_root"],
            }
            chain.append(block_dict)

        return chain

    def get_transaction_proof(self, block_index: int, tx_id):
        block = self.mongo.db.blockchain_blocks.find_one({"index": block_index})
        if not block:
            return None

        txs = list(
            self.mongo.db.blockchain_transactions
            .find({"block_id": block["_id"]})
            .sort("_id", 1)
        )

        transactions = [
            {
                "_id": tx["_id"],
                "sender": tx["sender"],
                "recipient": tx["recipient"],
                "amount": tx["amount"],
                "date": tx["date"]
            }
            for tx in txs
        ]

        tx_index = next((i for i, tx in enumerate(transactions) if str(tx["_id"]) == str(tx_id)), None)
        if tx_index is None:
            return None

        proof = self.get_merkle_proof(transactions, tx_index)
        return {
            "transaction": transactions[tx_index],
            "proof": proof,
            "merkle_root": block["merkle_root"]
        }

    def get_user_score(self, username: str):
        """
        Szybsze liczenie punktów użytkownika w MongoDB za pomocą agregacji.
        """

        # 1. Transakcje zatwierdzone
        pipeline_confirmed = [
            {"$match": {"$or": [{"sender": username}, {"recipient": username}]}},
            {"$group": {
                "_id": None,
                "sent": {"$sum": {"$cond": [{"$eq": ["$sender", username]}, "$amount", 0]}},
                "received": {"$sum": {"$cond": [{"$eq": ["$recipient", username]}, "$amount", 0]}}
            }}
        ]
        result = list(self.mongo.db.blockchain_transactions.aggregate(pipeline_confirmed))
        confirmed_score = 0
        if result:
            confirmed_score = result[0]["received"] - result[0]["sent"]

        # 2. Transakcje w mempoolu
        pipeline_mempool = [
            {"$match": {"$or": [{"sender": username}, {"recipient": username}]}},
            {"$group": {
                "_id": None,
                "sent": {"$sum": {"$cond": [{"$eq": ["$sender", username]}, "$amount", 0]}},
                "received": {"$sum": {"$cond": [{"$eq": ["$recipient", username]}, "$amount", 0]}}
            }}
        ]
        result_mempool = list(self.mongo.db.mempool_transactions.aggregate(pipeline_mempool))
        mempool_score = 0
        if result_mempool:
            mempool_score = result_mempool[0]["received"] - result_mempool[0]["sent"]

        return confirmed_score + mempool_score
